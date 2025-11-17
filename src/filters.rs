use std::collections::{HashMap, HashSet};

#[derive(Default, Clone)]
pub struct Filters {
    pub precisions: HashSet<String>,
    pub base_series: HashSet<String>,
    pub base_accel: HashSet<String>,
    pub m_values: HashSet<i32>,
    pub accel_params: HashMap<String, HashSet<String>>,
    pub series_params: HashMap<String, HashSet<String>>,
}