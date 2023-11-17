use tokio::fs::File;

use crate::Result;

pub struct Modifier {
    input_files: Vec<File>,
    output_file: File,
}

impl Modifier {
    pub async fn new(inputs: Vec<String>, output: String) -> Result<Self> {
        let mut input_files = Vec::new();
        for input in inputs {
            let file = File::open(input).await?;
            input_files.push(file);
        }
        let output_file = File::open(output).await?;
        Ok(Modifier {
            input_files,
            output_file,
        })
    }

    pub async fn merge(&self) -> Result<()> {
        todo!()
    }

    pub async fn prune(&self) -> Result<()> {
        todo!()
    }

    pub async fn trans_compression(&self) -> Result<()> {
        todo!()
    }

    pub async fn masking(&self) -> Result<()> {
        todo!()
    }
}
