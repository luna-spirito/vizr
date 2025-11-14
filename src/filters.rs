use serde::{Deserialize, Serialize};
use std::collections::HashSet;

#[derive(Default, Clone, Serialize, Deserialize)]
pub struct Filters {
    pub precisions: HashSet<String>,
    pub base_series: HashSet<String>,
    pub base_accel: HashSet<String>,
    pub m_values: HashSet<i32>,
    pub accel_params: std::collections::HashMap<String, HashSet<String>>,
    pub series_params: std::collections::HashMap<String, HashSet<String>>,
}

impl Filters {
    pub fn is_empty(&self) -> bool {
        self.precisions.is_empty()
            && self.base_series.is_empty()
            && self.base_accel.is_empty()
            && self.m_values.is_empty()
            && self.accel_params.is_empty()
            && self.series_params.is_empty()
    }
}