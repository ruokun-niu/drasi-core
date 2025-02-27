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

pub struct All {
    evaluator: Lazy<ExpressionEvaluator>,
}

impl Default for All {
    fn default() -> Self {
        Self::new()
    }
}

impl All {
    pub fn new() -> Self {
        All {
            evaluator: Lazy::new(|| {
                let function_registry = Arc::new(FunctionRegistry::new());
                let ari = Arc::new(InMemoryResultIndex::new());
                ExpressionEvaluator::new(function_registry, ari)
            }),
        }
    }
}

#[async_trait]
impl LazyScalarFunction for All {
    async fn call(
        &self,
        context: &ExpressionEvaluationContext,
        expression: &ast::FunctionExpression,
        args: &Vec<ast::Expression>,
    ) -> Result<VariableValue, FunctionError> {
        // This function should only have one argument
        // which is an IteratorExpression
        if args.len() != 1 { 
            return Err(FunctionError {
                function_name: expression.name.to_string(),
                error: FunctionEvaluationError::InvalidArgumentCount,
            });
        }
        let iterator = match &args[0] {
            Expression::IteratorExpression(iterator_expression) => iterator_expression,
            _ => {
                return Err(FunctionError {
                    function_name: expression.name.to_string(),
                    error: FunctionEvaluationError::InvalidArgument(0),
                });
            }
        };

        let items = match self.evaluator.evaluate_expression(context, &iterator.list_expression).await {
            Ok(items) => items,
            Err(_e) => {
                return Err(FunctionError {
                    function_name: expression.name.to_string(),
                    error: FunctionEvaluationError::InvalidType { expected: "A List".to_string() },
                });
            }
        };
        let items = match items {
            VariableValue::List(items) => items,
            VariableValue::Null => return Ok(VariableValue::Null),
            _ => {
                return Err(FunctionError {
                    function_name: expression.name.to_string(),
                    error: FunctionEvaluationError::InvalidType { expected: "A List or Null".to_string() },
                });
            }
        };

        let mut variables = context.clone_variables();

        let mut contains_null = false;
        for item in items {
            variables.insert(iterator.item_identifier.to_string().into(), item);
            if let Some(filter) = &iterator.filter {
                let local_context = ExpressionEvaluationContext::new(&variables, context.get_clock());
                match self.evaluator.evaluate_expression(&local_context, filter).await {
                    Ok(VariableValue::Bool(true)) => {},
                    Ok(VariableValue::Bool(false)) => {
                        return Ok(VariableValue::Bool(false));
                    },
                    Ok(VariableValue::Null) => {
                        contains_null = true;
                    },
                    Ok(_) => {
                        return Err(FunctionError {
                            function_name: expression.name.to_string(),
                            error: FunctionEvaluationError::InvalidType { expected: "A Boolean".to_string() },
                        });
                    },
                    Err(_e) => {
                        return Err(FunctionError {
                            function_name: expression.name.to_string(),
                            error: FunctionEvaluationError::InvalidType { expected: "A valid VariableValue variant".to_string() },
                        });
                    }
                }
            } 
        }

        // if at least one of the predicate evaluates to null AND
        // All other predicates evaluate to true, then the result is null
        if contains_null {
            return Ok(VariableValue::Null);
        }
        Ok(VariableValue::Bool(true))

        
    }
}



// FunctionExpression {
//     name: Arc::from("all"),
//     args: vec![
//         Expression::IteratorExpression(IteratorExpression {
//             item_identifier: Arc::from("x"),
//             list_expression: Box::new(Expression::FunctionExpression(FunctionExpression {
//                 name: Arc::from("nodes"),
//                 args: vec![Expression::UnaryExpression(UnaryExpression::Identifier(Arc::from("p")))],
//                 position_in_query: 0,
//             })),
//             filter: Some(Box::new(Expression::BinaryExpression(BinaryExpression::Lt(
//                 Box::new(Expression::UnaryExpression(UnaryExpression::Property {
//                     name: Arc::from("x"),
//                     key: Arc::from("age"),
//                 })),
//                 Box::new(Expression::UnaryExpression(UnaryExpression::Literal(Literal::Integer(60)))),
//             )))),
//             map_expression: None,
//         }),
//     ],
//     position_in_query: 1,
// }