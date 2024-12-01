extern crate core;

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use clap::{arg, Parser};
use indicatif::{ProgressBar, ProgressStyle};
use tokio::runtime;

use cc2p::{convert_to_parquet, find_files};
use parquet::file::properties::WriterProperties;
use parquet::basic::Compression;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Represents the folder path for CSV search.
    #[arg(default_value_t = String::from("*.csv"))]
    path: String,

    /// Represents the delimiter used in CSV files.
    #[arg(short, long, default_value_t = String::from(","))]
    delimiter: String,

    /// Represents whether to include the header in the CSV search column.
    #[arg(short, long, default_value_t = false)]
    no_header: bool,

    /// Number of worker threads to use for performing the task.
    #[arg(short, long, default_value_t = 1)]
    worker: u8,

    /// Number of rows to sample for inferring the schema.
    #[arg(short, long, default_value_t = 100)]
    sampling: u16,

    /// Optional output directory for Parquet files.
    #[arg(short, long)]
    output_dir: Option<String>,
}

struct ErrorData {
    file_path: String,
    error: String,
}

fn parse_delimiter(s: &str) -> Result<char, String> {
    match s {
        "\\t" => Ok('\t'),
        "\\n" => Ok('\n'),
        "\\r" => Ok('\r'),
        "\\," => Ok(','),
        "\\;" => Ok(';'),
        // Add more escape sequences if needed
        _ => {
            let mut chars = s.chars();
            if let Some(c) = chars.next() {
                if chars.next().is_none() {
                    Ok(c)
                } else {
                    Err(format!("Invalid delimiter: {}", s))
                }
            } else {
                Err("Delimiter cannot be empty".to_string())
            }
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let start = Instant::now();
    let path = args.path.as_str();
    let sampling_size = args.sampling;
    let has_header = !args.no_header;

    // Parse the delimiter
    let delimiter = parse_delimiter(args.delimiter.as_str())
        .map_err(|e| format!("Error parsing delimiter: {}", e))?;

    // Debug print to verify delimiter
    println!("Parsed delimiter: {:?}", delimiter);

    let output_dir = args.output_dir.as_deref();

    println!(
        "Program arguments\n path: {}\n delimiter: {:?}\n has header: {} \n worker count: {} \n sampling size: {} \n output directory: {:?}",
        path,
        delimiter,
        has_header,
        args.worker,
        sampling_size,
        output_dir
    );

    let errors = Arc::new(Mutex::new(Vec::<ErrorData>::new()));
    let files = find_files(path)?;

    let bar = ProgressBar::new(files.len().try_into().unwrap());
    bar.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.yellow/blue} {pos:>7}/{len:7} {msg}",
        )
        .unwrap(),
    );
    let bar = Arc::new(Mutex::new(bar));

    let runtime = runtime::Builder::new_multi_thread()
        .worker_threads(args.worker as usize)
        .enable_all()
        .build()?;

    runtime.block_on(async {
        let mut handles = vec![];

        for file in files {
            let bar = Arc::clone(&bar);
            let errors_clone = Arc::clone(&errors);

            let output_file = if let Some(output_dir) = output_dir {
                let mut output_path = PathBuf::from(output_dir);
                output_path.push(file.file_name().unwrap());
                output_path.set_extension("parquet");
                Some(output_path)
            } else {
                None
            };

            let h = tokio::spawn(async move {
                if let Err(err) = convert_to_parquet(&file, delimiter, has_header, sampling_size, output_file.as_ref()) {
                    let mut errors = errors_clone.lock().unwrap();
                    errors.push(ErrorData {
                        file_path: file.to_str().unwrap().to_string(),
                        error: err.to_string(),
                    });
                }
                bar.lock().unwrap().inc(1);
            });

            handles.push(h);
        }

        for handle in handles {
            let _ = handle.await;
        }
    });

    bar.lock().unwrap().finish();

    let errors = errors.lock().unwrap();
    for err_data in &*errors {
        println!(
            "File: {}  Error: {:?}\n",
            err_data.file_path, err_data.error
        );
    }

    let elapsed = start.elapsed();
    println!("Elapsed time {} ms", elapsed.as_millis());

    Ok(())
}
