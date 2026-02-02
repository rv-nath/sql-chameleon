use std::fs;
use std::process;

use clap::Parser;

use schema_conv::ast::Dialect;
use schema_conv::transpiler::SqlTranspiler;

/// SQL Schema Converter - converts SQL DDL between database dialects
#[derive(Parser)]
#[command(name = "schema-conv")]
#[command(about = "Convert SQL schemas between database dialects")]
struct Cli {
    /// Path to the source SQL file
    source_file: String,

    /// Source dialect
    #[arg(short = 'f', long, default_value = "mysql")]
    from: String,

    /// Target dialect
    #[arg(short = 't', long, default_value = "mysql")]
    to: String,

    /// Output file (if not specified, prints to console)
    #[arg(short = 'o', long)]
    output: Option<String>,
}

fn parse_dialect(name: &str) -> Result<Dialect, String> {
    match name.to_lowercase().as_str() {
        "mysql" => Ok(Dialect::MySQL),
        "oracle" => Ok(Dialect::Oracle),
        "postgresql" | "postgres" | "pg" => Ok(Dialect::PostgreSQL),
        "sqlite" => Ok(Dialect::SQLite),
        "sqlserver" | "mssql" => Ok(Dialect::SQLServer),
        _ => Err(format!(
            "Unknown dialect: '{}'. Supported: mysql, oracle, postgresql, sqlite, sqlserver",
            name
        )),
    }
}

fn main() {
    let cli = Cli::parse();

    // Parse dialect names
    let from_dialect = match parse_dialect(&cli.from) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    };

    let to_dialect = match parse_dialect(&cli.to) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Error: {}", e);
            process::exit(1);
        }
    };

    // Read source file
    let sql = match fs::read_to_string(&cli.source_file) {
        Ok(content) => content,
        Err(e) => {
            eprintln!("Error reading '{}': {}", cli.source_file, e);
            process::exit(1);
        }
    };

    // Convert
    let transpiler = SqlTranspiler::new();
    let output = match transpiler.convert(&sql, from_dialect, to_dialect) {
        Ok(result) => result,
        Err(e) => {
            eprintln!("Conversion error: {}", e);
            process::exit(1);
        }
    };

    // Write output
    match cli.output {
        Some(path) => match fs::write(&path, &output) {
            Ok(_) => {
                println!(
                    "Converted {} → {} written to '{}'",
                    from_dialect, to_dialect, path
                );
            }
            Err(e) => {
                eprintln!("Error writing '{}': {}", path, e);
                process::exit(1);
            }
        },
        None => {
            println!("{}", output);
        }
    }
}
