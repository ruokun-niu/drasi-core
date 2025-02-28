#![allow(clippy::unwrap_used)]
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

use std::sync::Arc;
use serde_json::json;
use crate::QueryTestConfig;

use drasi_core::{
    evaluation::{context::QueryPartEvaluationContext,variable_value::VariableValue},
    models::{Element, ElementMetadata, ElementPropertyMap, ElementReference, SourceChange},
    query::QueryBuilder,
};

macro_rules! variablemap {
    ($( $key: expr => $val: expr ),*) => {{
         let mut map = ::std::collections::BTreeMap::new();
         $( map.insert($key.to_string().into(), $val); )*
         map
    }}
  }

//   
pub fn test_query() -> &'static str {
    "MATCH 
        (n:Person)
    WHERE
        exists(n.surname)
    RETURN 
        n.surname as surname
    "
}

pub async fn test_exists_with_some_valid_properties(config: &(impl QueryTestConfig + Send)) {
    let query = {
        let mut builder = QueryBuilder::new(test_query());
        builder = config.config_query(builder).await;
        builder.build().await
    };

    let v0 = Element::Node {
        metadata: ElementMetadata {
            reference: ElementReference::new("test", "t1"),
            labels: Arc::new([Arc::from("Person")]),
            effective_from: 0,
        },
        properties: ElementPropertyMap::from(json!({"surname": "Smith"})),
    };

    let v1 = Element::Node {
        metadata: ElementMetadata {
            reference: ElementReference::new("test", "t2"),
            labels: Arc::new([Arc::from("Person")]),
            effective_from: 0,
        },
        properties: ElementPropertyMap::from(json!({"surname": "Brown"})),
    };

    let v2 = Element::Node {
        metadata: ElementMetadata {
            reference: ElementReference::new("test", "t3"),
            labels: Arc::new([Arc::from("Person")]),
            effective_from: 0,
        },
        properties: ElementPropertyMap::from(json!({"name": "Johnson"})),
    };

    

    let change = SourceChange::Insert {
        element: v0.clone(),
    };
    let result = query.process_source_change(change.clone()).await.unwrap();
    assert_eq!(result.len(), 1);
    assert!(result.contains(&QueryPartEvaluationContext::Adding {
        after: variablemap!(
            "surname" => VariableValue::from(json!("Smith"))
        )
    }));



    // v2 does not have the surname property, so it should not be included in the result
    let change = SourceChange::Insert {
        element: v2.clone(),
    };
    let result = query.process_source_change(change.clone()).await.unwrap();
    assert_eq!(result.len(), 0);

    // v1 has the surname property, so it should be included in the result
    let change = SourceChange::Insert {
        element: v1.clone(),
    };
    let result = query.process_source_change(change.clone()).await.unwrap();
    assert_eq!(result.len(), 1);

    let change = SourceChange::Update {
        element: Element::Node {
            metadata: ElementMetadata {
                reference: ElementReference::new("test", "t3"),
                labels: Arc::new([Arc::from("Person")]),
                effective_from: 0,
            },
            properties: ElementPropertyMap::from(json!({"surname": "Johnson"})),
        }
    };

    // Updating the v2 node (element_id=t3) to have the surname property should include it in the result
    let result = query.process_source_change(change.clone()).await.unwrap();
    assert_eq!(result.len(), 1);
    assert!(result.contains(&QueryPartEvaluationContext::Adding {
        after: variablemap!(
            "surname" => VariableValue::from(json!("Johnson"))
        )
    }));


    // testing with null inputs
    let change = SourceChange::Insert {
        element: Element::Node {
            metadata: ElementMetadata {
                reference: ElementReference::new("test", "t4"),
                labels: Arc::new([Arc::from("Person")]),
                effective_from: 0,
            },
            properties: ElementPropertyMap::from(json!({"surname": null})),
        }
    };

    let result = query.process_source_change(change.clone()).await.unwrap();
    assert_eq!(result.len(),0);

}


