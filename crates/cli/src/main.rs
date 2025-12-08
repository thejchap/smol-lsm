use anyhow::Result;
use clap::Parser;
use clap_verbosity_flag::{InfoLevel, Verbosity};
use frontend::serve_postgres;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// port number for the postgres server
    #[arg(long, default_value_t = 5432, value_parser = clap::value_parser!(u16).range(1024..))]
    port: u16,

    /// logging verbosity
    #[command(flatten)]
    verbosity: Verbosity<InfoLevel>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse();

    env_logger::Builder::new()
        .filter_level(args.verbosity.log_level_filter())
        .init();

    log::info!("serving on port: {}", args.port);

    serve_postgres(args.port).await
}
