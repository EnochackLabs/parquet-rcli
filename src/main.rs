use clap::Parser;
use clap::Subcommand;

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
    /// Merges multiple Parquet files into one. The command doesn't merge row groups,
    /// just places one after the other. When used to merge many small files, the
    /// resulting file will still contain small row groups, which usually leads to bad
    /// query performance
    Merge {
        /// Input files to merge
        input: Vec<String>,
        /// The output merged file
        output: String,
    },
    /// Prune column(s) in a Parquet file and save it to a new file. The columns left
    /// are not changed
    Prune {
        /// The input file
        input: String,
        /// The output pruned file
        output: String,
    },
    /// Translate the compression of a given Parquet file to a new compression one to a
    /// new Parquet file
    TransCompression {
        /// The input file
        input: String,
        /// The output pruned file
        output: String,
    },
    /// Replace columns in a given Parquet file with masked values and write to a new
    /// Parquet file.
    Masking {
        /// The input file
        input: String,
        /// The output pruned file
        output: String,
    },
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        Commands::Cat { limit, path } => Inspector::new(path).await?.print_data(limit).await?,
        Commands::RowCount { path } => Inspector::new(path).await?.row_count().await?,
        Commands::Schema { path } => Inspector::new(path).await?.print_schema().await?,
        Commands::Meta { path } => Inspector::new(path).await?.print_meta().await?,
        Commands::Size { uncompressed, path } => {
            Inspector::new(path).await?.print_size(uncompressed).await?
        }
        Commands::ColumnSize { path } => Inspector::new(path).await?.print_column_size().await?,
        Commands::Merge { input, output } => Modifier::new(input, output).await?.merge().await?,
        Commands::Prune { input, output } => {
            Modifier::new(vec![input], output).await?.prune().await?
        }
        Commands::TransCompression { input, output } => {
            Modifier::new(vec![input], output).await?.prune().await?
        }
        Commands::Masking { input, output } => {
            Modifier::new(vec![input], output).await?.masking().await?
        }
    }
    Ok(())
}
