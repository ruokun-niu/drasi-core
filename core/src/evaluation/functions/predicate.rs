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


use super::{Function, FunctionRegistry};
use std::sync::Arc;

mod all;
pub use all::All;

mod exists;
pub use exists::Exists;
pub trait RegisterPredicateFunctions {
    fn register_predicate_functions(&self);    
}

impl RegisterPredicateFunctions for FunctionRegistry {
    fn register_predicate_functions(&self) {
        self.register_function("all", Function::LazyScalar(Arc::new(All::new())));
        self.register_function("exists", Function::LazyScalar(Arc::new(Exists::new())));
    }
}