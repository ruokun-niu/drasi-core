// Copyright 2025 The Drasi Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use anyhow::{anyhow, Result};
use bytes::{Buf, BufMut, BytesMut};
use log::{debug, info, trace, warn};
use std::collections::HashMap;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

use super::config::SslMode;
use super::protocol::{
    parse_backend_message, AuthenticationMessage, BackendMessage, FrontendMessage, StartupMessage,
    TransactionStatus,
};
use super::scram::ScramClient;
use super::types::{ReplicationSlotInfo, StandbyStatusUpdate};
use postgres_protocol::authentication::sasl::ChannelBinding;

/// Combined async read+write trait for stream abstraction.
trait AsyncStream: AsyncRead + AsyncWrite + Unpin + Send + Sync {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send + Sync> AsyncStream for T {}

pub struct ReplicationConnection {
    stream: Box<dyn AsyncStream>,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
    parameters: HashMap<String, String>,
    process_id: Option<i32>,
    secret_key: Option<i32>,
    transaction_status: TransactionStatus,
    in_copy_mode: bool,
    /// TLS channel binding data (`tls-server-end-point` hash of the server cert).
    /// `Some(data)` when TLS is active and the peer certificate was available;
    /// `None` when TLS is not active or the peer cert could not be extracted.
    tls_server_end_point: Option<Vec<u8>>,
}

impl ReplicationConnection {
    pub async fn connect(
        host: &str,
        port: u16,
        database: &str,
        user: &str,
        password: &str,
        ssl_mode: SslMode,
    ) -> Result<Self> {
        info!("Connecting to PostgreSQL at {host}:{port} (ssl_mode={ssl_mode})");

        let mut tcp_stream = TcpStream::connect((host, port)).await?;
        tcp_stream.set_nodelay(true)?;

        // Negotiate SSL/TLS if needed
        let (stream, tls_server_end_point): (Box<dyn AsyncStream>, Option<Vec<u8>>) =
            match ssl_mode {
                SslMode::Disable => {
                    debug!("SSL disabled, using plain TCP");
                    (Box::new(tcp_stream), None)
                }
                SslMode::Prefer | SslMode::Require => {
                    // Send SSLRequest message
                    let ssl_request: [u8; 8] =
                        [0x00, 0x00, 0x00, 0x08, 0x04, 0xD2, 0x16, 0x2F];
                    tcp_stream.write_all(&ssl_request).await?;
                    tcp_stream.flush().await?;

                    let mut response = [0u8; 1];
                    tcp_stream.read_exact(&mut response).await?;

                    match response[0] {
                        b'S' => {
                            debug!("Server accepted SSL, upgrading connection");
                            let tls_connector = native_tls::TlsConnector::builder()
                                .danger_accept_invalid_certs(false)
                                .danger_accept_invalid_hostnames(false)
                                .build()
                                .map_err(|e| anyhow!("Failed to create TLS connector: {e}"))?;

                            let connector =
                                tokio_native_tls::TlsConnector::from(tls_connector);
                            let tls_stream = connector
                                .connect(host, tcp_stream)
                                .await
                                .map_err(|e| anyhow!("TLS handshake failed: {e}"))?;

                            // Extract tls-server-end-point channel binding data
                            let cb_data =
                                tls_stream.get_ref().tls_server_end_point().ok().flatten();
                            if cb_data.is_some() {
                                debug!(
                                    "Extracted tls-server-end-point channel binding data ({} bytes)",
                                    cb_data.as_ref().unwrap().len()
                                );
                            } else {
                                debug!(
                                    "Could not extract tls-server-end-point (peer cert unavailable)"
                                );
                            }

                            info!("TLS connection established to {host}:{port}");
                            (Box::new(tls_stream) as Box<dyn AsyncStream>, cb_data)
                        }
                        b'N' => {
                            if ssl_mode == SslMode::Require {
                                return Err(anyhow!(
                                    "Server does not support SSL but ssl_mode=require"
                                ));
                            }
                            debug!("Server declined SSL, continuing with plain TCP");
                            (Box::new(tcp_stream), None)
                        }
                        other => {
                            return Err(anyhow!(
                                "Unexpected SSL response from server: 0x{:02X}",
                                other
                            ));
                        }
                    }
                }
            };

        let mut conn = Self {
            stream,
            read_buffer: BytesMut::with_capacity(8192),
            write_buffer: BytesMut::with_capacity(8192),
            parameters: HashMap::new(),
            process_id: None,
            secret_key: None,
            transaction_status: TransactionStatus::Idle,
            in_copy_mode: false,
            tls_server_end_point,
        };

        conn.startup_replication(database, user, password).await?;

        Ok(conn)
    }

    async fn startup_replication(
        &mut self,
        database: &str,
        user: &str,
        password: &str,
    ) -> Result<()> {
        debug!("Starting replication protocol handshake");

        // Send startup message
        let startup = StartupMessage::new_replication(database, user);
        self.send_message(FrontendMessage::StartupMessage(startup))
            .await?;

        // Handle authentication
        loop {
            let msg = self.read_message().await?;
            match msg {
                BackendMessage::Authentication(auth) => {
                    match auth {
                        AuthenticationMessage::Ok => {
                            debug!("Authentication successful");
                            break;
                        }
                        AuthenticationMessage::CleartextPassword => {
                            debug!("Server requested cleartext password");
                            self.send_message(FrontendMessage::PasswordMessage(
                                password.to_string(),
                            ))
                            .await?;
                        }
                        AuthenticationMessage::MD5Password(_) => {
                            return Err(anyhow!(
                                "MD5 authentication is not supported (insecure). \
                                 Please configure PostgreSQL to use scram-sha-256 in pg_hba.conf"
                            ));
                        }
                        AuthenticationMessage::SASL(mechanisms) => {
                            debug!("Server offered SASL mechanisms: {mechanisms:?}");

                            let has_scram =
                                mechanisms.contains(&"SCRAM-SHA-256".to_string());
                            let has_scram_plus =
                                mechanisms.contains(&"SCRAM-SHA-256-PLUS".to_string());

                            let (channel_binding, mechanism) = if has_scram_plus {
                                match &self.tls_server_end_point {
                                    Some(data) => {
                                        debug!("Using SCRAM-SHA-256-PLUS with tls-server-end-point");
                                        (
                                            ChannelBinding::tls_server_end_point(data.clone()),
                                            "SCRAM-SHA-256-PLUS",
                                        )
                                    }
                                    None => {
                                        debug!("Server offered SCRAM-SHA-256-PLUS but no CB data; falling back to SCRAM-SHA-256");
                                        (ChannelBinding::unsupported(), "SCRAM-SHA-256")
                                    }
                                }
                            } else if has_scram {
                                match &self.tls_server_end_point {
                                    Some(_) => {
                                        debug!("Using SCRAM-SHA-256 with CB unrequested (y,,)");
                                        (ChannelBinding::unrequested(), "SCRAM-SHA-256")
                                    }
                                    None => {
                                        debug!("Using SCRAM-SHA-256 without CB (n,,)");
                                        (ChannelBinding::unsupported(), "SCRAM-SHA-256")
                                    }
                                }
                            } else {
                                return Err(anyhow!(
                                    "No supported SASL mechanisms in: {mechanisms:?}"
                                ));
                            };

                            debug!("Password/token length: {} bytes", password.len());
                            let mut scram_client =
                                ScramClient::new(user, password, channel_binding);

                            // Send SASLInitialResponse
                            let client_first = scram_client.client_first_message();
                            debug!(
                                "SCRAM client-first-message ({} bytes): {:?}",
                                client_first.len(),
                                String::from_utf8_lossy(&client_first)
                            );
                            self.send_sasl_initial_response_bytes(mechanism, &client_first)
                                .await?;

                            // Continue SASL exchange
                            loop {
                                let sasl_msg = self.read_message().await?;
                                match sasl_msg {
                                    BackendMessage::Authentication(
                                        AuthenticationMessage::SASLContinue(data),
                                    ) => {
                                        debug!(
                                            "SCRAM server-first-message ({} bytes)",
                                            data.len()
                                        );
                                        scram_client
                                            .process_server_first_message(&data)?;
                                        let client_final =
                                            scram_client.client_final_message();
                                        self.send_sasl_response_bytes(&client_final)
                                            .await?;
                                    }
                                    BackendMessage::Authentication(
                                        AuthenticationMessage::SASLFinal(data),
                                    ) => {
                                        scram_client.verify_server_final(&data)?;
                                        debug!("SCRAM authentication successful");
                                        break;
                                    }
                                    BackendMessage::ErrorResponse(err) => {
                                        return Err(anyhow!(
                                            "SASL authentication failed: {}",
                                            err.message
                                        ));
                                    }
                                    _ => {
                                        warn!(
                                            "Unexpected message during SASL: {sasl_msg:?}"
                                        );
                                    }
                                }
                            }
                        }
                        _ => {
                            return Err(anyhow!("Unsupported authentication method"));
                        }
                    }
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(anyhow!("Authentication failed: {}", err.message));
                }
                _ => {
                    warn!("Unexpected message during authentication: {msg:?}");
                }
            }
        }

        // Wait for ReadyForQuery
        loop {
            let msg = self.read_message().await?;
            match msg {
                BackendMessage::BackendKeyData {
                    process_id,
                    secret_key,
                } => {
                    self.process_id = Some(process_id);
                    self.secret_key = Some(secret_key);
                    debug!("Received backend key data: pid={process_id}");
                }
                BackendMessage::ParameterStatus { name, value } => {
                    debug!("Parameter: {name} = {value}");
                    self.parameters.insert(name, value);
                }
                BackendMessage::ReadyForQuery(status) => {
                    self.transaction_status = status;
                    debug!("Connection ready, status: {status:?}");
                    break;
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(anyhow!("Startup failed: {}", err.message));
                }
                BackendMessage::NoticeResponse(notice) => {
                    info!("Notice: {}", notice.message);
                }
                _ => {
                    warn!("Unexpected message during startup: {msg:?}");
                }
            }
        }

        Ok(())
    }

    pub async fn identify_system(&mut self) -> Result<HashMap<String, String>> {
        debug!("Sending IDENTIFY_SYSTEM command");

        self.send_message(FrontendMessage::Query("IDENTIFY_SYSTEM".to_string()))
            .await?;

        let mut system_info = HashMap::new();

        loop {
            let msg = self.read_message().await?;
            match msg {
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(row) => {
                    if row.len() >= 4 {
                        if let Some(Some(v)) = row.first() {
                            system_info
                                .insert("systemid".into(), String::from_utf8_lossy(v).into());
                        }
                        if let Some(Some(v)) = row.get(1) {
                            system_info
                                .insert("timeline".into(), String::from_utf8_lossy(v).into());
                        }
                        if let Some(Some(v)) = row.get(2) {
                            system_info
                                .insert("xlogpos".into(), String::from_utf8_lossy(v).into());
                        }
                        if let Some(Some(v)) = row.get(3) {
                            system_info
                                .insert("dbname".into(), String::from_utf8_lossy(v).into());
                        }
                    }
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::ReadyForQuery(status) => {
                    self.transaction_status = status;
                    break;
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(anyhow!("IDENTIFY_SYSTEM failed: {}", err.message));
                }
                _ => {
                    warn!("Unexpected message during IDENTIFY_SYSTEM: {msg:?}");
                }
            }
        }

        Ok(system_info)
    }

    pub async fn create_replication_slot(
        &mut self,
        slot_name: &str,
        temporary: bool,
    ) -> Result<ReplicationSlotInfo> {
        debug!("Creating replication slot: {slot_name}");

        let query = if temporary {
            format!("CREATE_REPLICATION_SLOT {slot_name} TEMPORARY LOGICAL pgoutput")
        } else {
            format!("CREATE_REPLICATION_SLOT {slot_name} LOGICAL pgoutput")
        };

        self.send_message(FrontendMessage::Query(query)).await?;

        let mut slot_info = ReplicationSlotInfo {
            slot_name: slot_name.to_string(),
            consistent_point: String::new(),
            snapshot_name: None,
            output_plugin: "pgoutput".to_string(),
        };

        loop {
            let msg = self.read_message().await?;
            match msg {
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(row) => {
                    if row.len() >= 4 {
                        if let Some(Some(cp)) = row.get(1) {
                            slot_info.consistent_point =
                                String::from_utf8_lossy(cp).to_string();
                        }
                        if let Some(Some(sn)) = row.get(2) {
                            slot_info.snapshot_name =
                                Some(String::from_utf8_lossy(sn).to_string());
                        }
                    }
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::ReadyForQuery(status) => {
                    self.transaction_status = status;
                    break;
                }
                BackendMessage::ErrorResponse(err) => {
                    if err.message.contains("already exists") {
                        debug!("Replication slot already exists: {slot_name}");
                        loop {
                            let drain_msg = self.read_message().await?;
                            if let BackendMessage::ReadyForQuery(status) = drain_msg {
                                self.transaction_status = status;
                                break;
                            }
                        }
                        return self.get_replication_slot_info(slot_name).await;
                    }
                    return Err(anyhow!("CREATE_REPLICATION_SLOT failed: {}", err.message));
                }
                _ => {
                    warn!("Unexpected message during CREATE_REPLICATION_SLOT: {msg:?}");
                }
            }
        }

        Ok(slot_info)
    }

    pub async fn get_replication_slot_info(
        &mut self,
        slot_name: &str,
    ) -> Result<ReplicationSlotInfo> {
        debug!("Querying existing replication slot: {slot_name}");

        let slot_name_escaped = slot_name.replace('\'', "''");
        let query = format!(
            "SELECT slot_name, confirmed_flush_lsn, restart_lsn, plugin \
             FROM pg_replication_slots WHERE slot_name = '{slot_name_escaped}'"
        );

        self.send_message(FrontendMessage::Query(query)).await?;

        let mut slot_info = ReplicationSlotInfo {
            slot_name: slot_name.to_string(),
            consistent_point: "0/0".to_string(),
            snapshot_name: None,
            output_plugin: "pgoutput".to_string(),
        };
        let mut found_row = false;

        loop {
            let msg = self.read_message().await?;
            match msg {
                BackendMessage::RowDescription(_) => {}
                BackendMessage::DataRow(row) => {
                    found_row = true;
                    if row.len() >= 4 {
                        if let Some(Some(v)) = row.get(1) {
                            let lsn = String::from_utf8_lossy(v).to_string();
                            if !lsn.is_empty() {
                                slot_info.consistent_point = lsn;
                            }
                        }
                        if slot_info.consistent_point == "0/0" {
                            if let Some(Some(v)) = row.get(2) {
                                let lsn = String::from_utf8_lossy(v).to_string();
                                if !lsn.is_empty() {
                                    slot_info.consistent_point = lsn;
                                }
                            }
                        }
                        if let Some(Some(v)) = row.get(3) {
                            slot_info.output_plugin =
                                String::from_utf8_lossy(v).to_string();
                        }
                    }
                }
                BackendMessage::CommandComplete(_) => {}
                BackendMessage::ReadyForQuery(status) => {
                    self.transaction_status = status;
                    break;
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(anyhow!(
                        "Failed to query replication slot: {}",
                        err.message
                    ));
                }
                _ => {
                    warn!("Unexpected message during slot query: {msg:?}");
                }
            }
        }

        if !found_row {
            return Err(anyhow!("Replication slot not found: {slot_name}"));
        }

        info!(
            "Using existing replication slot: {slot_name} at LSN {}",
            slot_info.consistent_point
        );
        Ok(slot_info)
    }

    pub async fn start_replication(
        &mut self,
        slot_name: &str,
        start_lsn: Option<u64>,
        options: HashMap<String, String>,
    ) -> Result<()> {
        debug!("Starting replication from slot: {slot_name}");

        let mut query = format!("START_REPLICATION SLOT {slot_name} LOGICAL");

        if let Some(lsn) = start_lsn {
            query.push_str(&format!(" {}", format_lsn(lsn)));
        } else {
            query.push_str(" 0/0");
        }

        if !options.is_empty() {
            query.push_str(" (");
            let opts: Vec<String> =
                options.iter().map(|(k, v)| format!("{k} '{v}'")).collect();
            query.push_str(&opts.join(", "));
            query.push(')');
        }

        self.send_message(FrontendMessage::Query(query)).await?;

        // Wait for CopyBothResponse
        loop {
            let msg = self.read_message().await?;
            match msg {
                BackendMessage::CopyBothResponse => {
                    debug!("Entered COPY BOTH mode for replication");
                    self.in_copy_mode = true;
                    break;
                }
                BackendMessage::ErrorResponse(err) => {
                    return Err(anyhow!("START_REPLICATION failed: {}", err.message));
                }
                BackendMessage::ReadyForQuery(_) => {
                    debug!("Received ReadyForQuery before entering COPY mode");
                }
                _ => {
                    debug!("Message during START_REPLICATION: {msg:?}");
                }
            }
        }

        Ok(())
    }

    pub async fn read_replication_message(&mut self) -> Result<BackendMessage> {
        if !self.in_copy_mode {
            return Err(anyhow!("Not in COPY mode"));
        }

        self.read_message().await
    }

    pub async fn send_standby_status(&mut self, status: StandbyStatusUpdate) -> Result<()> {
        if !self.in_copy_mode {
            return Err(anyhow!("Not in COPY mode"));
        }

        let timestamp = chrono::Utc::now().timestamp_micros() - 946_684_800_000_000;

        self.send_message(FrontendMessage::StandbyStatusUpdate {
            write_lsn: status.write_lsn,
            flush_lsn: status.flush_lsn,
            apply_lsn: status.apply_lsn,
            timestamp,
            reply: if status.reply_requested { 1 } else { 0 },
        })
        .await
    }

    async fn send_message(&mut self, msg: FrontendMessage) -> Result<()> {
        self.write_buffer.clear();
        msg.encode(&mut self.write_buffer)?;

        self.stream.write_all(&self.write_buffer).await?;
        self.stream.flush().await?;

        trace!("Sent message: {msg:?}");
        Ok(())
    }

    async fn send_sasl_initial_response_bytes(
        &mut self,
        mechanism: &str,
        data: &[u8],
    ) -> Result<()> {
        self.send_message(FrontendMessage::SASLInitialResponse {
            mechanism: mechanism.to_string(),
            data: data.to_vec(),
        })
        .await
    }

    async fn send_sasl_response_bytes(&mut self, data: &[u8]) -> Result<()> {
        self.send_message(FrontendMessage::SASLResponse(data.to_vec()))
            .await
    }

    async fn read_message(&mut self) -> Result<BackendMessage> {
        loop {
            if let Some(msg) = self.try_parse_message()? {
                trace!("Received message: {msg:?}");
                return Ok(msg);
            }

            let mut temp_buf = vec![0u8; 4096];
            let n = self.stream.read(&mut temp_buf).await?;
            if n == 0 {
                return Err(anyhow!("Connection closed by server"));
            }

            self.read_buffer.extend_from_slice(&temp_buf[..n]);
        }
    }

    fn try_parse_message(&mut self) -> Result<Option<BackendMessage>> {
        if self.read_buffer.len() < 5 {
            return Ok(None);
        }

        let msg_type = self.read_buffer[0];
        let length = u32::from_be_bytes([
            self.read_buffer[1],
            self.read_buffer[2],
            self.read_buffer[3],
            self.read_buffer[4],
        ]) as usize;

        if length < 4 {
            return Err(anyhow!("Invalid message length: {length}"));
        }

        let total_length = 1 + length;

        if self.read_buffer.len() < total_length {
            return Ok(None);
        }

        let body = self.read_buffer[5..total_length].to_vec();
        self.read_buffer.advance(total_length);

        let msg = parse_backend_message(msg_type, &body)?;
        Ok(Some(msg))
    }

    pub async fn close(mut self) -> Result<()> {
        if self.in_copy_mode {
            let _ = self.send_message(FrontendMessage::CopyDone).await;
        }
        let _ = self.send_message(FrontendMessage::Terminate).await;
        let _ = self.stream.shutdown().await;
        Ok(())
    }
}

fn format_lsn(lsn: u64) -> String {
    format!("{:X}/{:X}", lsn >> 32, lsn & 0xFFFFFFFF)
}

#[allow(dead_code)]
fn parse_lsn(lsn_str: &str) -> Result<u64> {
    let parts: Vec<&str> = lsn_str.split('/').collect();
    if parts.len() != 2 {
        return Err(anyhow!("Invalid LSN format: {lsn_str}"));
    }

    let high = u64::from_str_radix(parts[0], 16)?;
    let low = u64::from_str_radix(parts[1], 16)?;

    Ok((high << 32) | low)
}
