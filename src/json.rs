use std::collections::HashMap;
use std::fmt::Display;

use parquet::file::metadata::{
    ColumnChunkMetaData, FileMetaData, ParquetMetaData, RowGroupMetaData,
};
use parquet::file::statistics::Statistics;
use serde_json::{from_str, json, Value};

pub trait ToJsonValue {
    fn to_json_value(&self) -> Value;
}

impl ToJsonValue for FileMetaData {
    fn to_json_value(&self) -> Value {
        let version = self.version();
        let num_rows = self.num_rows();
        let created_by = self.created_by();
        let mut kvs = HashMap::new();
        if let Some(kv_metadata) = self.key_value_metadata() {
            for kv in kv_metadata {
                if let Some(value) = &kv.value {
                    let jvalue: Value = from_str(value).unwrap_or(Value::String(value.clone()));
                    kvs.insert(&kv.key, jvalue);
                }
            }
        }
        json!({
            "version": version,
            "num_rows": num_rows,
            "created_by": created_by,
            "metadata": kvs
        })
    }
}

impl ToJsonValue for Statistics {
    fn to_json_value(&self) -> Value {
        let mut value = match self {
            Statistics::Boolean(vs) => {
                let min = vs.min();
                let max = vs.max();
                json!({
                    "min": min,
                    "max": max,
                })
            }
            Statistics::Int32(vs) => {
                let min = vs.min();
                let max = vs.max();
                json!({
                    "min": min,
                    "max": max,
                })
            }
            Statistics::Int64(vs) => {
                let min = vs.min();
                let max = vs.max();
                json!({
                    "min": min,
                    "max": max,
                })
            }
            Statistics::Int96(vs) => {
                let min = vs.min().to_string();
                let max = vs.max().to_string();
                json!({
                    "min": min,
                    "max": max,
                })
            }
            Statistics::Float(vs) => {
                let min = vs.min();
                let max = vs.max();
                json!({
                    "min": min,
                    "max": max,
                })
            }
            Statistics::Double(vs) => {
                let min = vs.min();
                let max = vs.max();
                json!({
                    "min": min,
                    "max": max,
                })
            }
            Statistics::ByteArray(vs) => {
                let min = vs.min().to_string();
                let max = vs.max().to_string();
                json!({
                    "min": min,
                    "max": max,
                })
            }
            Statistics::FixedLenByteArray(vs) => {
                let min = vs.min().to_string();
                let max = vs.max().to_string();
                json!({
                    "min": min,
                    "max": max,
                })
            }
        };
        let distinct_count = self.distinct_count();
        let null_count = self.null_count();
        let is_min_max_deprecated = self.is_min_max_deprecated();
        value
            .as_object_mut()
            .unwrap()
            .insert("distinct_count".to_string(), Value::from(distinct_count));
        value
            .as_object_mut()
            .unwrap()
            .insert("null_count".to_string(), Value::from(null_count));
        value.as_object_mut().unwrap().insert(
            "is_min_max_deprecated".to_string(),
            Value::from(is_min_max_deprecated),
        );
        value
    }
}

impl ToJsonValue for ColumnChunkMetaData {
    fn to_json_value(&self) -> Value {
        let column_type = self.column_type().to_string();
        let column_path = self.column_path().string();
        let encodings = self
            .encodings()
            .iter()
            .map(|e| e.to_string())
            .collect::<Vec<String>>();
        let file_path = self.file_path();
        let file_offset = self.file_offset();
        let num_values = self.num_values();
        let compression = self.compression().to_string();
        let compressed_size = self.compressed_size();
        let uncompressed_size = self.uncompressed_size();
        let data_page_offset = self.data_page_offset();
        let index_page_offset = self.index_page_offset();
        let dict_page_offset = self.dictionary_page_offset();
        let statistics = self.statistics().map(|s| s.to_json_value());
        let bloomfilter_offset = self.bloom_filter_offset();
        let bloomfilter_length = self.bloom_filter_length();
        let offset_index_offset = self.offset_index_offset();
        let offset_index_length = self.offset_index_length();
        let column_index_offset = self.column_index_offset();
        let column_index_length = self.column_index_length();
        json!({
            "column_type": column_type,
            "column_path": column_path,
            "encodings": encodings,
            "file_path": file_path,
            "file_offset": file_offset,
            "num_values": num_values,
            "compression": compression,
            "compressed_size": compressed_size,
            "uncompressed_size": uncompressed_size,
            "data_page_offset": data_page_offset,
            "index_page_offset": index_page_offset,
            "dict_page_offset": dict_page_offset,
            "statistics": statistics,
            "bloomfilter_offset": bloomfilter_offset,
            "bloomfilter_length": bloomfilter_length,
            "offset_index_offset": offset_index_offset,
            "offset_index_length": offset_index_length,
            "column_index_offset": column_index_offset,
            "column_index_length": column_index_length,
        })
    }
}

impl ToJsonValue for RowGroupMetaData {
    fn to_json_value(&self) -> Value {
        let ordinal = self.ordinal();
        let total_byte_size = self.total_byte_size();
        let num_rows = self.num_rows();
        let num_columns = self.num_columns();
        let columns = self
            .columns()
            .iter()
            .map(|c| c.to_json_value())
            .collect::<Vec<Value>>();
        json!({
            "ordinal": ordinal,
            "total_byte_size": total_byte_size,
            "num_rows": num_rows,
            "num_columns": num_columns,
            "columns": columns,
        })
    }
}

impl ToJsonValue for ParquetMetaData {
    fn to_json_value(&self) -> Value {
        let file_metadata = self.file_metadata().to_json_value();
        let num_row_groups = self.num_row_groups();
        let row_groups = self
            .row_groups()
            .iter()
            .map(|rg| rg.to_json_value())
            .collect::<Vec<Value>>();
        json!({
            "file_metadata": file_metadata,
            "num_row_groups": num_row_groups,
            "row_groups": row_groups,
        })
    }
}
