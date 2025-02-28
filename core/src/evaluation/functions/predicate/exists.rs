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

use async_trait::async_trait;
use drasi_query_ast::ast::{self, Expression};
use once_cell::sync::Lazy;
use std::sync::Arc;
use crate::in_memory_index::in_memory_result_index::InMemoryResultIndex;
use crate::evaluation::functions::{FunctionRegistry, LazyScalarFunction};
use crate::evaluation::{ExpressionEvaluationContext, ExpressionEvaluator, FunctionError, FunctionEvaluationError};

use crate::evaluation::variable_value::VariableValue;

pub struct Exists {
    evaluator: Lazy<ExpressionEvaluator>,
}

impl Default for Exists {
    fn default() -> Self {
        Self::new()
    }
}

impl Exists {
    pub fn new() -> Self {
        Exists {
            evaluator: Lazy::new(|| {
                let function_registry = Arc::new(FunctionRegistry::new());
                let ari = Arc::new(InMemoryResultIndex::new());
                ExpressionEvaluator::new(function_registry, ari)
            }),
        }
    }
}

#[async_trait]
impl LazyScalarFunction for Exists {
    async fn call(
        &self,
        context: &ExpressionEvaluationContext,
        expression: &ast::FunctionExpression,
        args: &Vec<ast::Expression>,
    ) -> Result<VariableValue, FunctionError> {
        // This implementation is based on OpenCypher9
        // The input to this function must be a property expression e.g. exists(n.surname)
        if args.len() != 1 {
            return Err(FunctionError {
                function_name: expression.name.to_string(),
                error: FunctionEvaluationError::InvalidArgumentCount,
            });
        }
        let unary_expression = match &args[0] {
            Expression::UnaryExpression(unary_expression) => unary_expression,
            _ => {
                return Err(FunctionError {
                    function_name: expression.name.to_string(),
                    error: FunctionEvaluationError::InvalidArgument(0),
                });
            }
        };

        let property_expression_result = match self.evaluator.evaluate_expression(context, &ast::Expression::UnaryExpression(unary_expression.clone())).await {
            Ok(property_expression_result) => property_expression_result,
            Err(_e) => {
                return Err(FunctionError {
                    function_name: expression.name.to_string(),
                    error: FunctionEvaluationError::InvalidArgument(0),
                });
            }
        };

        match property_expression_result {
            VariableValue::Null => return Ok(VariableValue::Null),
            _ => return Ok(VariableValue::Bool(true)),
        };


    }
}