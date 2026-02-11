use std::sync::Arc;

use aisle::{ExprRowFilter, PruneRequest};
use arrow_array::{Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use datafusion_expr::{Expr, col, lit};
use parquet::arrow::ArrowWriter;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::{
    ParquetRecordBatchReaderBuilder, RowFilter as ParquetRowFilter,
};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

#[derive(Debug, Default, Clone, Copy)]
struct ScanMetrics {
    kept_row_groups: usize,
    rows_in_kept_groups: usize,
    rows_emitted: usize,
    output_columns: usize,
    decoded_cell_proxy: usize,
    output_bytes_proxy: usize,
}

fn create_wide_data(
    row_groups: usize,
    rows_per_group: usize,
    payload_columns: usize,
) -> (Bytes, ParquetMetaData, Arc<Schema>) {
    let mut fields = Vec::with_capacity(2 + payload_columns);
    fields.push(Field::new("key", DataType::Int64, false));
    fields.push(Field::new("group_mod", DataType::Int64, false));
    for idx in 0..payload_columns {
        fields.push(Field::new(
            format!("payload_{idx:02}"),
            DataType::Int64,
            false,
        ));
    }
    let schema = Arc::new(Schema::new(fields));

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(rows_per_group)
        .build();

    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();

    for row_group_idx in 0..row_groups {
        let start = (row_group_idx * rows_per_group) as i64;
        let keys: Vec<i64> = (0..rows_per_group).map(|row| start + row as i64).collect();
        // Every row group has min=0/max=9 for group_mod, so row-group pruning keeps all groups.
        let group_mod: Vec<i64> = (0..rows_per_group).map(|row| (row % 10) as i64).collect();

        let mut columns: Vec<Arc<dyn arrow_array::Array>> = Vec::with_capacity(2 + payload_columns);
        columns.push(Arc::new(Int64Array::from(keys)));
        columns.push(Arc::new(Int64Array::from(group_mod)));

        for payload_idx in 0..payload_columns {
            let scale = (payload_idx + 1) as i64;
            let values: Vec<i64> = (0..rows_per_group)
                .map(|row| {
                    let key = start + row as i64;
                    key.wrapping_mul(scale) ^ (payload_idx as i64)
                })
                .collect();
            columns.push(Arc::new(Int64Array::from(values)));
        }

        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();
    let bytes = Bytes::from(buffer);
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&bytes)
        .unwrap();
    (bytes, metadata, schema)
}

fn run_scan(
    bytes: &Bytes,
    metadata: &ParquetMetaData,
    schema: &Arc<Schema>,
    predicate: &Expr,
    projection_columns: Option<&[&str]>,
) -> ScanMetrics {
    let prune = PruneRequest::new(metadata, schema)
        .with_df_predicate(predicate)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .prune();

    let parquet_schema = metadata.file_metadata().schema_descr();
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .unwrap()
        .with_row_groups(prune.row_groups().to_vec())
        .with_row_filter(ParquetRowFilter::new(vec![Box::new(ExprRowFilter::new(
            predicate.clone(),
            parquet_schema,
        ))]));

    if let Some(columns) = projection_columns {
        builder = builder.with_projection(ProjectionMask::columns(
            parquet_schema,
            columns.iter().copied(),
        ));
    }

    let reader = builder.build().unwrap();

    let mut metrics = ScanMetrics {
        kept_row_groups: prune.row_groups().len(),
        rows_in_kept_groups: prune
            .row_groups()
            .iter()
            .map(|idx| metadata.row_group(*idx).num_rows() as usize)
            .sum(),
        ..ScanMetrics::default()
    };

    for batch in reader {
        let batch = batch.unwrap();
        metrics.rows_emitted += batch.num_rows();
        metrics.output_columns = batch.num_columns();
        metrics.decoded_cell_proxy += batch.num_rows() * batch.num_columns();
        metrics.output_bytes_proxy += batch
            .columns()
            .iter()
            .map(|array| array.get_array_memory_size())
            .sum::<usize>();
    }

    metrics
}

fn bench_projection_pushdown(c: &mut Criterion) {
    let row_groups = 40;
    let rows_per_group = 4096;
    let payload_columns = 48;
    let (bytes, metadata, schema) = create_wide_data(row_groups, rows_per_group, payload_columns);

    let predicate = col("group_mod").eq(lit(3i64));
    let requested_projection = ["key"];

    let no_projection_metrics = run_scan(&bytes, &metadata, &schema, &predicate, None);
    let manual_projection_metrics = run_scan(
        &bytes,
        &metadata,
        &schema,
        &predicate,
        Some(&requested_projection),
    );

    println!(
        "Projection benchmark workload: row_groups={}, rows_per_group={}, columns_total={}",
        row_groups,
        rows_per_group,
        schema.fields().len()
    );
    println!(
        "No projection: kept_rg={}, rows_read={}, rows_emitted={}, output_cols={}, decoded_cells={}, output_bytes={}",
        no_projection_metrics.kept_row_groups,
        no_projection_metrics.rows_in_kept_groups,
        no_projection_metrics.rows_emitted,
        no_projection_metrics.output_columns,
        no_projection_metrics.decoded_cell_proxy,
        no_projection_metrics.output_bytes_proxy,
    );
    println!(
        "Manual projection ref: kept_rg={}, rows_read={}, rows_emitted={}, output_cols={}, decoded_cells={}, output_bytes={}",
        manual_projection_metrics.kept_row_groups,
        manual_projection_metrics.rows_in_kept_groups,
        manual_projection_metrics.rows_emitted,
        manual_projection_metrics.output_columns,
        manual_projection_metrics.decoded_cell_proxy,
        manual_projection_metrics.output_bytes_proxy,
    );

    let mut group = c.benchmark_group("projection_pushdown_wide_scan");

    group.bench_function("aisle_projection_pipeline", |b| {
        b.iter(|| black_box(run_scan(&bytes, &metadata, &schema, &predicate, None)))
    });

    group.bench_function("aisle_projection_manual_reference", |b| {
        b.iter(|| {
            black_box(run_scan(
                &bytes,
                &metadata,
                &schema,
                &predicate,
                Some(&requested_projection),
            ))
        })
    });

    group.finish();
}

criterion_group!(benches, bench_projection_pushdown);
criterion_main!(benches);
