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

use crate::evaluation::context::QueryVariables;
use crate::evaluation::functions::FunctionRegistry;
use crate::evaluation::variable_value::integer::Integer;
use crate::evaluation::variable_value::VariableValue;
use crate::evaluation::{EvaluationError, InstantQueryClock};
use crate::evaluation::{ExpressionEvaluationContext, ExpressionEvaluator};
use crate::in_memory_index::in_memory_result_index::InMemoryResultIndex;
use std::sync::Arc;
use std::collections::BTreeMap;


#[tokio::test]
async fn test_all_when_some_elements_unsatisfied_returns_false() {
    let expr = "all(x IN $param1 WHERE x.age < 60)";
    let expr = drasi_query_cypher::parse_expression(expr).unwrap();

    let function_registry = Arc::new(FunctionRegistry::new());
    let ari = Arc::new(InMemoryResultIndex::new());
    let evaluator = ExpressionEvaluator::new(function_registry.clone(), ari.clone());

    let mut variables = QueryVariables::new();
    let node_list = vec![
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("foo".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(11)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("bar".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(86)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("baz".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(3)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("qux".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(121)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("quux".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(-45)));
            map
        }),
    ];

    variables.insert("param1".into(), VariableValue::List(node_list));

    {
        let context =
            ExpressionEvaluationContext::new(&variables, Arc::new(InstantQueryClock::new(0, 0)));
        assert_eq!(
            evaluator
                .evaluate_expression(&context, &expr)
                .await
                .unwrap(),
            VariableValue::Bool(false)
        );
        
    }
    
}

#[tokio::test]
async fn test_all_with_all_elements_satisfied_returns_true() {
    let expr = "all(x IN $param1 WHERE x.age < 60)";
    let expr = drasi_query_cypher::parse_expression(expr).unwrap();

    let function_registry = Arc::new(FunctionRegistry::new());
    let ari = Arc::new(InMemoryResultIndex::new());
    let evaluator = ExpressionEvaluator::new(function_registry.clone(), ari.clone());

    let mut variables = QueryVariables::new();
    let node_list = vec![
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("foo".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(11)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("bar".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(56)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("baz".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(3)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("qux".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(45)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("quux".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(12)));
            map
        }),
    ];

    variables.insert("param1".into(), VariableValue::List(node_list));

    {
        let context =
            ExpressionEvaluationContext::new(&variables, Arc::new(InstantQueryClock::new(0, 0)));
        assert_eq!(
            evaluator
                .evaluate_expression(&context, &expr)
                .await
                .unwrap(),
            VariableValue::Bool(true)
        );
        
    }
}

#[tokio::test]
async fn test_all_with_null_list_returns_null() {
    let expr = "all(x IN $param1 WHERE x.age < 60)";
    let expr = drasi_query_cypher::parse_expression(expr).unwrap();

    let function_registry = Arc::new(FunctionRegistry::new());
    let ari = Arc::new(InMemoryResultIndex::new());
    let evaluator = ExpressionEvaluator::new(function_registry.clone(), ari.clone());

    let mut variables = QueryVariables::new();
    variables.insert("param1".into(), VariableValue::Null);

    {
        let context =
            ExpressionEvaluationContext::new(&variables, Arc::new(InstantQueryClock::new(0, 0)));
        assert_eq!(
            evaluator
                .evaluate_expression(&context, &expr)
                .await
                .unwrap(),
            VariableValue::Null
        );
        
    }
    
}

#[tokio::test]
async fn test_all_with_some_elements_null_returns_null() {
    let expr = "all(x IN $param1 WHERE x.age < 60)";
    let expr = drasi_query_cypher::parse_expression(expr).unwrap();

    let function_registry = Arc::new(FunctionRegistry::new());
    let ari = Arc::new(InMemoryResultIndex::new());
    let evaluator = ExpressionEvaluator::new(function_registry.clone(), ari.clone());

    let mut variables = QueryVariables::new();
    let node_list = vec![
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("foo".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(11)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("bar".to_string()));
            map.insert("age".to_string(), VariableValue::Null);
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("baz".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(3)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("qux".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(45)));
            map
        }),
    ];
    variables.insert("param1".into(), VariableValue::List(node_list));

    {
        let context =
            ExpressionEvaluationContext::new(&variables, Arc::new(InstantQueryClock::new(0, 0)));
        assert_eq!(
            evaluator
                .evaluate_expression(&context, &expr)
                .await
                .unwrap(),
            VariableValue::Null
        );
        
    }
}

#[tokio::test]
async fn test_all_with_some_elements_null_and_some_elements_false_returns_false() {
    let expr = "all(x IN $param1 WHERE x.age < 60)";
    let expr = drasi_query_cypher::parse_expression(expr).unwrap();

    let function_registry = Arc::new(FunctionRegistry::new());
    let ari = Arc::new(InMemoryResultIndex::new());
    let evaluator = ExpressionEvaluator::new(function_registry.clone(), ari.clone());

    let mut variables = QueryVariables::new();
    let node_list = vec![
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("foo".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(11)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("bar".to_string()));
            map.insert("age".to_string(), VariableValue::Null);
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("baz".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(3)));
            map
        }),
        VariableValue::Object({
            let mut map = BTreeMap::new();
            map.insert("name".to_string(), VariableValue::String("qux".to_string()));
            map.insert("age".to_string(), VariableValue::Integer(Integer::from(121)));
            map
        }),
    ];
    variables.insert("param1".into(), VariableValue::List(node_list));

    {
        let context =
            ExpressionEvaluationContext::new(&variables, Arc::new(InstantQueryClock::new(0, 0)));
        assert_eq!(
            evaluator
                .evaluate_expression(&context, &expr)
                .await
                .unwrap(),
            VariableValue::Bool(false)
        );
        
    }
}