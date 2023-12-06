use std::error::Error;
use std::io;

use arrow::error::ArrowError;
use parquet::errors::ParquetError;

pub mod inspector;
mod json;
pub mod modifier;

pub type Result<T> = std::result::Result<T, CliError>;

#[derive(Debug)]
pub enum CliError {
    General(String),
    ArrowError(ArrowError),
    ParquetError(ParquetError),
    External(Box<dyn Error + Send + Sync>),
}

impl From<ArrowError> for CliError {
    fn from(error: ArrowError) -> Self {
        Self::ArrowError(error)
    }
}

impl From<ParquetError> for CliError {
    fn from(error: ParquetError) -> Self {
        Self::ParquetError(error)
    }
}

impl From<io::Error> for CliError {
    fn from(e: io::Error) -> Self {
        CliError::External(Box::new(e))
    }
}

impl From<serde_json::error::Error> for CliError {
    fn from(e: serde_json::Error) -> Self {
        CliError::External(Box::new(e))
    }
}
