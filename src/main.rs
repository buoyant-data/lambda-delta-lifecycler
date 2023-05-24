/*
 * This AWS Lambda function relies on S3 Event Notifications to identify when an object has been
 * deleted and therefore should be removed from the _delta_log/
 */

use aws_lambda_events::s3::S3Event;
use lambda_runtime::{service_fn, Error, LambdaEvent};
use log::*;
use serde_json::json;
use serde_json::Value;

#[tokio::main]
async fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    info!("Starting the Lambda runtime");
    let func = service_fn(func);
    lambda_runtime::run(func).await
}

/*
 * Lambda function handler
 */
async fn func<'a>(_event: LambdaEvent<S3Event>) -> Result<Value, Error> {
    let location = std::env::var("DATALAKE_LOCATION")?;

    Ok(json!({
        "message": format!("Modified the table at: {}", location)
    }))
}
