use drasi_lib::channels::{QueryResult, ResultDiff};
use drasi_lib::profiling::ProfilingMetadata;
use serde_json::json;
use std::collections::HashMap;

fn update(grouping_keys: Option<Vec<String>>) -> ResultDiff {
    ResultDiff::Update {
        data: json!({"FloorId": "F_000_000", "AvgTemperature": 5000.0}),
        before: json!({"FloorId": "F_000_000", "AvgTemperature": 4999.0}),
        after: json!({"FloorId": "F_000_000", "AvgTemperature": 5000.0}),
        grouping_keys,
        row_signature: 13_660_005_145_781_501_189,
    }
}

fn result(diffs: Vec<ResultDiff>, profiling: bool) -> QueryResult {
    let mut result = QueryResult::new(
        "outbox-codec".to_string(),
        43_327,
        chrono::DateTime::from_timestamp(1_700_000_000, 123).unwrap(),
        diffs,
        HashMap::from([("result_count".to_string(), json!(1))]),
    );
    if profiling {
        result.profiling = Some(ProfilingMetadata::new());
    }
    result
}

#[test]
fn positional_update_without_grouping_keys_reproduces_recovery_error() {
    let original = result(vec![update(None)], true);
    let bytes = rmp_serde::to_vec(&original).unwrap();
    let error = rmp_serde::from_slice::<QueryResult>(&bytes).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("invalid type: integer `13660005145781501189`, expected a sequence"),
        "Unexpected error: {error}"
    );
    println!("Legacy positional outbox codec: {error}");

    let original = result(vec![update(Some(vec!["FloorId".to_string()]))], true);
    let bytes = rmp_serde::to_vec(&original).unwrap();
    let restored = rmp_serde::from_slice::<QueryResult>(&bytes).unwrap();
    assert_eq!(
        serde_json::to_value(original).unwrap(),
        serde_json::to_value(restored).unwrap()
    );
}

#[test]
fn named_query_results_round_trip_all_diff_variants_and_optional_fields() {
    let diffs = vec![
        ResultDiff::Add {
            data: json!({"value": 1}),
            row_signature: u64::MAX,
        },
        ResultDiff::Delete {
            data: json!({"value": 1}),
            row_signature: u64::MAX,
        },
        update(None),
        update(Some(vec![])),
        update(Some(vec!["FloorId".to_string()])),
        ResultDiff::Aggregation {
            before: None,
            after: json!({"count": 1}),
            row_signature: u64::MAX,
        },
        ResultDiff::Aggregation {
            before: Some(json!({"count": 1})),
            after: json!({"count": 2}),
            row_signature: u64::MAX,
        },
        ResultDiff::Noop,
    ];
    for profiling in [false, true] {
        let mut batches: Vec<Vec<ResultDiff>> =
            diffs.iter().cloned().map(|diff| vec![diff]).collect();
        batches.push(diffs.clone());
        batches.push(vec![]);
        for batch in batches {
            let original = result(batch, profiling);
            let bytes = rmp_serde::to_vec_named(&original).unwrap();
            let restored = rmp_serde::from_slice::<QueryResult>(&bytes).unwrap_or_else(|error| {
                panic!("Named round trip failed for {original:?}: {error}")
            });
            assert_eq!(
                serde_json::to_value(&original).unwrap(),
                serde_json::to_value(restored).unwrap()
            );
        }
    }
}
