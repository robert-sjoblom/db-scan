use std::{
    fs::File,
    io::{BufWriter, Write},
    path::Path,
};

use super::OutputRow;

/// CSV writer that streams rows as they arrive
pub struct CsvWriter {
    writer: BufWriter<File>,
}

impl CsvWriter {
    pub fn new(path: &str) -> std::io::Result<Self> {
        let file = File::create(Path::new(path))?;
        let mut writer = BufWriter::new(file);
        // Write header
        writeln!(
            writer,
            "status,cluster,primary,replicas,lag_bytes,reason,details_json"
        )?;
        Ok(Self { writer })
    }

    pub fn write_row(&mut self, row: &OutputRow) -> std::io::Result<()> {
        writeln!(
            self.writer,
            "{},{},{},{},{},{},\"{}\"",
            row.status.as_str(),
            row.cluster,
            row.primary,
            row.replicas,
            row.lag.map(|l| l.to_string()).unwrap_or_default(),
            row.reason,
            row.details_json.replace('"', "\"\"")
        )
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
