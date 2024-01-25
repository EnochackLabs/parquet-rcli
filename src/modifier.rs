use std::fs::File;

use arrow::array::RecordBatchReader;
use arrow::datatypes::SchemaRef;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use parquet::file::properties::WriterPropertiesBuilder;
use parquet::file::reader::{FileReader, SerializedFileReader};

use crate::{CliError, Result};

pub struct Modifier {
    input_paths: Vec<String>,
    output_path: String,
    schema: SchemaRef,
}

impl Modifier {
    pub fn new(inputs: Vec<String>, output: String) -> Result<Self> {
        let mut readers = Vec::new();
        for input in &inputs {
            let file = File::open(input)?;
            let reader = ParquetRecordBatchReaderBuilder::try_new(file)?.build()?;
            readers.push(reader);
        }
        let schema = readers[0].schema();
        for reader in &readers[1..] {
            if reader.schema() != schema {
                return Err(CliError::General(
                    "Schemas of input files are different".to_string(),
                ));
            }
        }
        Ok(Modifier {
            input_paths: inputs,
            output_path: output,
            schema,
        })
    }

    pub fn rewrite(
        &self,
        mut properties_builder: WriterPropertiesBuilder,
        columns: Vec<String>,
    ) -> Result<()> {
        let serialized_reader = SerializedFileReader::new(File::open(&self.input_paths[0])?)?;
        let kv_md = serialized_reader
            .metadata()
            .file_metadata()
            .key_value_metadata()
            .cloned();
        properties_builder = properties_builder.set_key_value_metadata(kv_md);
        let properties = properties_builder.build();
        let mut writer = ArrowWriter::try_new(
            File::create(&self.output_path)?,
            self.schema.clone(),
            Some(properties),
        )?;
        for path in &self.input_paths {
            let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.build()?;
            for batch in reader.into_iter() {
                let batch = batch?;
                writer.write(&batch)?;
            }
        }
        writer.close()?;
        Ok(())
    }
}
