/// Options for controlling metadata pruning behavior
#[derive(Clone, Debug)]
pub struct PruneOptions {
    enable_page_index: bool,
    emit_roaring: bool,
    enable_bloom_filter: bool,
    allow_truncated_byte_array_ordering: bool,
}

impl PruneOptions {
    /// Create a new builder for PruneOptions
    ///
    /// # Example
    /// ```
    /// use aisle::PruneOptions;
    ///
    /// let options = PruneOptions::builder()
    ///     .enable_page_index(true)
    ///     .emit_roaring(false)
    ///     .build();
    /// ```
    pub fn builder() -> PruneOptionsBuilder {
        PruneOptionsBuilder::default()
    }

    /// Check if page index pruning is enabled
    pub fn enable_page_index(&self) -> bool {
        self.enable_page_index
    }

    /// Check if roaring bitmap output is enabled
    pub fn emit_roaring(&self) -> bool {
        self.emit_roaring
    }

    /// Check if bloom filter pruning is enabled
    pub fn enable_bloom_filter(&self) -> bool {
        self.enable_bloom_filter
    }

    /// Check if ordering predicates can use truncated byte array stats
    pub fn allow_truncated_byte_array_ordering(&self) -> bool {
        self.allow_truncated_byte_array_ordering
    }
}

impl Default for PruneOptions {
    fn default() -> Self {
        Self {
            enable_page_index: true,
            emit_roaring: true,
            enable_bloom_filter: true,
            allow_truncated_byte_array_ordering: false,
        }
    }
}

/// Builder for PruneOptions
#[derive(Clone, Debug, Default)]
pub struct PruneOptionsBuilder {
    enable_page_index: Option<bool>,
    emit_roaring: Option<bool>,
    enable_bloom_filter: Option<bool>,
    allow_truncated_byte_array_ordering: Option<bool>,
}

impl PruneOptionsBuilder {
    /// Enable or disable page index pruning (default: true)
    ///
    /// When enabled, uses Parquet page index metadata for finer-grained
    /// pruning at the page level within row groups.
    pub fn enable_page_index(mut self, value: bool) -> Self {
        self.enable_page_index = Some(value);
        self
    }

    /// Enable or disable roaring bitmap output (default: true)
    ///
    /// When enabled, PruneResult will include a RoaringBitmap representation
    /// of the row selection. This provides a compact, efficient format for
    /// representing selected rows.
    ///
    /// # Limitations
    ///
    /// RoaringBitmap is limited to datasets with ≤ 4,294,967,295 rows
    /// (u32::MAX). For larger datasets, the RoaringBitmap output will be
    /// `None` and a message will be printed to stderr. In this case, use
    /// `RowSelection` directly, which has no size limitations.
    ///
    /// # Example
    ///
    /// ```
    /// use aisle::PruneOptions;
    ///
    /// // For most datasets (< 4.2B rows), roaring bitmap is useful
    /// let options = PruneOptions::builder().emit_roaring(true).build();
    ///
    /// // For very large datasets, you might disable it
    /// let options = PruneOptions::builder()
    ///     .emit_roaring(false) // Skip roaring, use RowSelection only
    ///     .build();
    /// ```
    pub fn emit_roaring(mut self, value: bool) -> Self {
        self.emit_roaring = Some(value);
        self
    }

    /// Enable or disable bloom filter pruning (default: true)
    ///
    /// Bloom filters require access to the Parquet file contents to read
    /// the bloom filter bitsets. Use `prune_metadata_with_async_reader` or
    /// `Pruner::prune_with_async_reader` to supply an async reader.
    pub fn enable_bloom_filter(mut self, value: bool) -> Self {
        self.enable_bloom_filter = Some(value);
        self
    }

    /// Allow ordering predicates to use truncated BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY stats (default:
    /// false).
    ///
    /// When disabled, byte array ordering requires type-defined (unsigned) column order
    /// and exact min/max statistics. When enabled, truncation is allowed but column order
    /// is still respected.
    pub fn allow_truncated_byte_array_ordering(mut self, value: bool) -> Self {
        self.allow_truncated_byte_array_ordering = Some(value);
        self
    }

    /// Build the PruneOptions
    pub fn build(self) -> PruneOptions {
        PruneOptions {
            enable_page_index: self.enable_page_index.unwrap_or(true),
            emit_roaring: self.emit_roaring.unwrap_or(true),
            enable_bloom_filter: self.enable_bloom_filter.unwrap_or(true),
            allow_truncated_byte_array_ordering: self
                .allow_truncated_byte_array_ordering
                .unwrap_or(false),
        }
    }
}
