use std::sync::Arc;
use std::time::Duration;
use object_store::ObjectStore;
use object_store::path::Path;
use s3::load_aws_creds;
use slatedb::config::DbOptions;
use slatedb::db::Db;
use slatedb::error::SlateDBError;
use crate::args::{DbBenchArgs, DbBenchCommand, parse_args};
use crate::db_bench::DbBench;

#[cfg(feature = "db_bench")] mod args;
mod db_bench;
#[cfg(feature = "aws")] mod s3;

#[cfg(not(feature = "db_bench"))]
fn main() {
    panic!("db_bench not enabled")
}

fn load_object_store(args: &DbBenchArgs) -> Result<Arc<dyn ObjectStore>, SlateDBError> {
    #[cfg(feature = "aws")]
    {
        let (aws_key, aws_secret) = load_aws_creds();
        Ok(Arc::new(
            object_store::aws::AmazonS3Builder::new()
                .with_access_key_id(aws_key.as_str())
                .with_secret_access_key(aws_secret.as_str())
                .with_bucket_name(args.bucket.as_str())
                .with_region(args.region.as_str())
                .build()?,
        ))
    }
    #[cfg(not(feature = "aws"))]
    {
        panic!("feature aws must be enabled to run db bench")
    }
}


#[cfg(feature = "db_bench")]
#[tokio::main]
async fn main() {
    let args: DbBenchArgs = parse_args();
    let mut db_options = DbOptions::default();
    db_options.wal_enabled = !args.disable_wal.unwrap_or(false);
    db_options.flush_interval = args
        .flush_ms
        .map(|i| Duration::from_millis(i as u64))
        .unwrap_or(db_options.flush_interval);
    let path = Path::from(args.path.as_str());
    let os = load_object_store(&args).expect("failed to open object store");
    let db = Arc::new(
        Db::open_with_opts(
            path.clone(),
            db_options,
            os.clone(),
        ).await.expect("failed to open db")
    );

    let bench = match args.command {
        DbBenchCommand::Write(write) => {
            let key_gen_supplier = write.key_gen_supplier();
            let write_options = write.write_options();
            DbBench::write(
                key_gen_supplier,
                write.val_len,
                write_options,
                write.write_rate,
                write.write_tasks,
                write.num_rows,
                write.duration.map(|d| Duration::from_millis(d as u64)),
                db.clone()
            )
        }
    };

    bench.run().await;

    println!("{}", args.path);
}