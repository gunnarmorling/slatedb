use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(about, long_about=None)]
struct DbBenchArgs {
    #[arg(short, long)]
    bucket: String,
    #[arg(short, long)]
    path: String,
    #[arg(short, long)]
    flush_ms: Option<u32>,
    flush_sz: Option<u32>,
    command: Option<DbBenchCommand>
}

#[derive(Subcommand)]
enum DbBenchCommand {
    Write {
        #[arg(short, long)]
        duration: Option<u32>,
        #[arg(short, long)]
        key_distribution: KeyDistribution,
        #[arg(short, long)]
        key_len: u32,
        #[arg(short, long)]
        val_len: u32,
    }
}

enum KeyDistribution {
    Random,
}

fn main() {

}