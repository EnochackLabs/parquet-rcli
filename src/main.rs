use std::str::FromStr;

use clap::Subcommand;
use clap::{Args, Parser};
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use parquet_rcli::inspector::*;
use parquet_rcli::modifier::Modifier;
use parquet_rcli::Result;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Prints the content of a Parquet file. The output contains only the data, no metadata is
    /// displayed
    Cat {
        /// The maximum number of records to print. 0 means no limit
        #[arg(short, long, default_value = "0")]
        limit: usize,
        #[arg(short, long, value_delimiter = ',')]
        columns: Vec<String>,
        /// The path to the Parquet file
        path: String,
    },
    /// Prints the count of rows in the Parquet file
    RowCount {
        /// The path to the Parquet file
        path: String,
    },
    /// Prints the schema of the Parquet file
    Schema {
        /// The path to the Parquet file
        path: String,
    },
    /// Prints the metadata of the Parquet file
    Meta {
        /// The path to the Parquet file
        path: String,
    },
    /// Prints the size of the Parquet file
    Size {
        /// Uncompressed size
        #[arg(short, long, required = false)]
        uncompressed: bool,
        /// The path to the Parquet file
        path: String,
    },
    /// Prints out the size in bytes and ratio of column(s) in the Parquet file
    ColumnSize {
        /// The path to the Parquet file
        path: String,
    },
    /// Rewrite one or more Parquet files to a new Parquet file
    Rewrite {
        /// Input file(s) separated by comma(s)
        #[arg(short, long, required = true, value_delimiter = ',')]
        input: Vec<String>,
        /// The output file
        #[arg(short, long, required = true)]
        output: String,
        /// Compression codec
        #[arg(short, long, required = false)]
        compression_codec: Option<String>,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Commands::Cat {
            limit,
            columns,
            path,
        } => {
            Inspector::new(path)
                .await?
                .print_data(columns, limit)
                .await?
        }
        Commands::RowCount { path } => Inspector::new(path).await?.row_count().await?,
        Commands::Schema { path } => Inspector::new(path).await?.print_schema().await?,
        Commands::Meta { path } => Inspector::new(path).await?.print_meta().await?,
        Commands::Size { uncompressed, path } => {
            Inspector::new(path).await?.print_size(uncompressed).await?
        }
        Commands::ColumnSize { path } => Inspector::new(path).await?.print_column_size().await?,
        Commands::Rewrite {
            input,
            output,
            compression_codec,
        } => {
            let mut properties_builder = WriterProperties::builder();
            if let Some(value) = compression_codec {
                properties_builder =
                    properties_builder.set_compression(Compression::from_str(&value)?);
            }
            Modifier::new(input, output)?.rewrite(properties_builder)?
        }
    }
    Ok(())
}
