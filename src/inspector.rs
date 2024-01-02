use std::borrow::Borrow;
use std::fs::File;
use std::io::stdout;

use indexmap::IndexMap;
use parquet::arrow::arrow_reader::RowGroups;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::schema::printer::print_schema;
use parquet::schema::types::{Type, TypePtr};
use serde::{Deserialize, Serialize};
use serde_json::{to_string_pretty, Map};

use crate::json::ToJsonValue;
use crate::Result;

pub struct Inspector {
    serialized_reader: SerializedFileReader<File>,
    metadata: ParquetMetaData,
}

impl Inspector {
    pub async fn new(path: String) -> Result<Self> {
        let file = File::open(path)?;
        let serialized_reader = SerializedFileReader::new(file)?;
        let metadata = serialized_reader.metadata().clone();
        Ok(Inspector {
            serialized_reader,
            metadata,
        })
    }

    pub async fn print_data(mut self, columns: Vec<String>, limit: usize) -> Result<()> {
        let schema = self.metadata.file_metadata().schema().clone();
        let fields = schema
            .get_fields()
            .iter()
            .filter(|f| columns.contains(&f.name().to_string()))
            .cloned()
            .collect::<Vec<TypePtr>>();
        let projection = if columns.is_empty() {
            None
        } else {
            Some(
                Type::group_type_builder("spark_schema")
                    .with_fields(fields)
                    .build()?,
            )
        };
        for (i, row) in self.serialized_reader.get_row_iter(projection)?.enumerate() {
            if limit != 0 && i == limit {
                break;
            }
            let value = row?.to_json_value();
            println!("{}", value.to_string());
        }
        Ok(())
    }

    pub async fn print_schema(&self) -> Result<()> {
        Ok(print_schema(
            &mut stdout(),
            self.metadata.file_metadata().schema(),
        ))
    }

    pub async fn row_count(&self) -> Result<()> {
        let row_count = self
            .metadata
            .row_groups()
            .iter()
            .fold(0, |count, row_group| count + row_group.num_rows());
        Ok(println!("{row_count}"))
    }

    pub async fn print_meta(&self) -> Result<()> {
        let json = self.metadata.to_json_value();
        let json = to_string_pretty(&json)?;
        println!("{json}");
        Ok(())
    }

    pub async fn print_size(&self, uncompressed: bool) -> Result<()> {
        let size = self.metadata.row_groups().iter().fold(0, |s, rg| {
            s + if uncompressed {
                rg.total_byte_size()
            } else {
                rg.compressed_size()
            }
        });
        println!("{size}");
        Ok(())
    }

    pub async fn print_column_size(&self) -> Result<()> {
        let mut column_map = IndexMap::new();
        for rg in self.metadata.row_groups() {
            for cc in rg.columns() {
                let column_path = cc.column_path().string();
                let compressed_size = cc.compressed_size() as usize;
                let uncompressed_size = cc.uncompressed_size() as usize;
                if column_map.contains_key(&column_path) {
                    let column_size: &mut ColumnSize = column_map.get_mut(&column_path).unwrap();
                    column_size.add(compressed_size, uncompressed_size);
                } else {
                    let column_size = ColumnSize::new(compressed_size, uncompressed_size);
                    column_map.insert(column_path, column_size);
                }
            }
        }
        let mut map = Map::new();
        for (k, v) in column_map {
            map.insert(k, serde_json::to_value(v)?);
        }
        println!("{}", to_string_pretty(&map)?);
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct ColumnSize {
    compressed_size: usize,
    uncompressed_size: usize,
    compression_ratio: Option<f32>,
}

impl ColumnSize {
    pub fn new(compressed_size: usize, uncompressed_size: usize) -> Self {
        ColumnSize {
            compressed_size,
            uncompressed_size,
            compression_ratio: if compressed_size != 0 && uncompressed_size != 0 {
                Some(compressed_size as f32 / uncompressed_size as f32)
            } else {
                None
            },
        }
    }

    pub fn add(&mut self, compressed_size: usize, uncompressed_size: usize) -> () {
        self.compressed_size += compressed_size;
        self.uncompressed_size += uncompressed_size;
        if self.compressed_size != 0 && self.uncompressed_size != 0 {
            self.compression_ratio =
                Some(self.compressed_size as f32 / self.uncompressed_size as f32)
        }
    }
}
