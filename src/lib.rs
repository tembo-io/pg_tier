use std::path::Path;

//use aws_config::meta::region::ProvideRegion;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::create_bucket::{CreateBucketError, CreateBucketOutput};
use aws_sdk_s3::operation::put_object::{PutObjectError, PutObjectOutput};
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketLocationConstraint, CreateBucketConfiguration};
use aws_sdk_s3::{config::Credentials, config::Region, Client, Config};
use pgrx::prelude::*;

pgrx::pg_module_magic!();

extension_sql!(
    r#"
create table server_credential(
    cred_id integer not null UNIQUE default(1),
    created_on timestamptz not null,
    updated_on timestamptz,
    user_name text not null,
    bucket text not null,
    access_key text not null,
    secret_key text not null,
    region text not null,
    fdw_server_name text not null,
    fdw_server_user_created bool not null,
    CONSTRAINT only_one_row_check CHECK(cred_id = 1)
    );
"#,
    name = "create_spi_server_credential_table",
);

extension_sql!(
    r#"
create table source(
    src_oid oid not null UNIQUE,
    src_relnamespace oid not null,
    src_inhparent oid,
    src_orig_relname name not null,
    src_new_relname name not null,
    src_state text not null,
    src_enabled boolean not null,
    src_dropped boolean not null,
    src_created_on timestamptz not null
    );
"#,
    name = "create_spi_source_table",
);

extension_sql!(
    r#"
create table target(
    tgt_oid oid not null UNIQUE,
    tgt_relname name not null,
    tgt_relnamespace oid not null,
    tgt_src_oid oid not null,
    tgt_tier_state text not null,
    tgt_ddl text not null,
    tgt_tier_dir text not null,
    tgt_src_partition_bound text,
    tgt_created_on timestamptz not null
    );
"#,
    name = "create_spi_target_table",
);

extension_sql!(
    r#"
       SELECT set_tier_cloud_config();
    "#,
    name = "cloud_init_spi_server_table",
);

extension_sql_file!("../sql/pg_tier.sql");

#[pg_extern]
#[tokio::main]
async fn create_top_level_bucket(
    bucket_name: String,
    access_key: String,
    secret_key: String,
    region: String,
) -> Result<bool, aws_sdk_s3::Error> {
    let provider_name = "tembo";
    let reg_clone = region.clone();
    let creds = Credentials::new(access_key, secret_key, None, None, provider_name);
    let config = Config::builder()
        .region(Region::new(region))
        .behavior_version_latest()
        .credentials_provider(creds)
        .build();

    let client = Client::from_conf(config);
    let resp = client.list_buckets().send().await?;
    let buckets = resp.buckets();
    let mut present: bool = false;
    let binding = bucket_name.clone();
    let bn: &str = binding.as_str();

    //println!("Bucket count {}", buckets.len().to_string());
    for bucket in buckets {
        //        println!("{}", bucket.name().unwrap_or_default());
        if bucket.name.clone().unwrap() == bucket_name {
            present = true;
            break;
        }
    }

    if !present {
        //println!("Calling Make Bucket");
        let resp = make_bucket(&client, bn, reg_clone.as_str()).await?;
        println!("Make Bucket Response {:?}", resp);
        Ok(true)
    } else {
        Ok(true)
    }
}

#[pg_extern]
#[tokio::main]
async fn upload_parquet_schema_file(
    bucket_name: String,
    access_key: String,
    secret_key: String,
    region: String,
    local_file_path: String,
    parquet_file_name: String,
) -> Result<(), aws_sdk_s3::Error> {
    let provider_name = "tembo";
    let creds = Credentials::new(access_key, secret_key, None, None, provider_name);
    let config = Config::builder()
        .region(Region::new(region))
        .behavior_version_latest()
        .credentials_provider(creds)
        .build();

    let client = Client::from_conf(config);
    let abs_file_path = local_file_path + "/" + parquet_file_name.as_str();
    let resp = upload_object(
        &client,
        bucket_name.as_str(),
        abs_file_path.as_str(),
        parquet_file_name.as_str(),
    )
    .await?;
    println!("Upload Object Response {:?}", resp);
    Ok(())
}

async fn make_bucket(
    client: &Client,
    bucket: &str,
    region: &str,
) -> Result<CreateBucketOutput, SdkError<CreateBucketError>> {
    let constraint = BucketLocationConstraint::from(region);
    let cfg = CreateBucketConfiguration::builder()
        .location_constraint(constraint)
        .build();

    Ok(client
        .create_bucket()
        .create_bucket_configuration(cfg)
        .bucket(bucket)
        .send()
        .await?)
}

pub async fn upload_object(
    client: &Client,
    bucket_name: &str,
    file_name: &str,
    key: &str,
) -> Result<PutObjectOutput, SdkError<PutObjectError>> {
    let body = ByteStream::from_path(Path::new(file_name)).await;
    Ok(client
        .put_object()
        .bucket(bucket_name)
        .key(key)
        .body(body.unwrap())
        .send()
        .await?)
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn test_create_tier_table() {
        assert_eq!(
            "Hello, pg_tier",
            crate::create_tier_table("db", "table", "col")
        );
    }
}

/// This module is required by `cargo pgrx test` invocations.
/// It must be visible at the root of your extension crate.
#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {
        // perform one-off initialization when the pg_test framework starts
    }

    pub fn postgresql_conf_options() -> Vec<&'static str> {
        // return any postgresql.conf settings that are required for your tests
        vec![]
    }
}
