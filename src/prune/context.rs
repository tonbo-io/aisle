use std::collections::HashMap;

use arrow_schema::Schema;
use parquet::{
    bloom_filter::Sbbf, file::metadata::ParquetMetaData, schema::types::SchemaDescriptor,
};

pub(crate) struct RowGroupContext<'a> {
    pub(crate) metadata: &'a ParquetMetaData,
    pub(crate) schema: &'a Schema,
    pub(crate) column_lookup: &'a HashMap<String, usize>,
    pub(crate) row_group_idx: usize,
    pub(crate) bloom_filters: Option<HashMap<usize, Sbbf>>,
    pub(crate) options: &'a super::options::PruneOptions,
}

impl RowGroupContext<'_> {
    pub(crate) fn bloom_for_column(&self, column: &str) -> Option<&Sbbf> {
        let col_idx = *self.column_lookup.get(column)?;
        self.bloom_filters.as_ref()?.get(&col_idx)
    }
}

pub(crate) fn build_column_lookup(schema: &SchemaDescriptor) -> HashMap<String, usize> {
    let mut name_counts: HashMap<String, usize> = HashMap::new();
    for column in schema.columns() {
        *name_counts.entry(column.name().to_string()).or_insert(0) += 1;
    }

    let mut lookup = HashMap::new();
    for (idx, column) in schema.columns().iter().enumerate() {
        let path = column.path().string();
        lookup.insert(path, idx);
    }
    for (idx, column) in schema.columns().iter().enumerate() {
        let name = column.name();
        if name_counts.get(name).copied().unwrap_or(0) == 1 {
            lookup.entry(name.to_string()).or_insert(idx);
        }
    }
    lookup
}
