/*
 * This AWS Lambda function relies on S3 Event Notifications to identify when an object has been
 * deleted and therefore should be removed from the _delta_log/
 */

use aws_lambda_events::s3::{S3Event, S3EventRecord};
use deltalake::action::*;
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
async fn func<'a>(event: LambdaEvent<S3Event>) -> Result<Value, Error> {
    let location = std::env::var("DATALAKE_LOCATION")?;
    let table = deltalake::open_table(&location).await?;

    Ok(json!({
        "message": format!("Modified the table at: {}", location)
    }))
}

/*
 * Generate a Vec of Delta Lake actions which can be committed to the transaction log
 */
fn remove_actions_for(records: &[S3EventRecord]) -> Result<Vec<Action>, anyhow::Error> {
    let actions: Vec<Action> = records
        .iter()
        // only bother with the record if the key is present
        .filter(|record| record.s3.object.key.is_some())
        // urldecode the key name and pass along
        .map(|record| {
            (
                record.s3.object.key.clone().unwrap(),
                record.event_time.timestamp_millis(),
            )
        })
        // Pass only the information needed along
        .map(|(key, timestamp)| {
            Action::remove(Remove {
                path: urlencoding::decode(&key).unwrap().into_owned(),
                deletion_timestamp: Some(timestamp),
                data_change: true,
                ..Default::default()
            })
        })
        .collect();
    Ok(actions)
}

/*
 * Validate a given event for delete operations that match the provided location
 */
fn validated_deletes(location: &str, event: S3Event) -> Result<Vec<S3EventRecord>, anyhow::Error> {
    let mut results = vec![];

    for record in event.records.into_iter() {
        if let Some(bucket) = &record.s3.bucket.name {
            if let Some(key) = &record.s3.object.key {
                let key = urlencoding::decode(key)?;
                let uri = format!("s3://{}/{}", bucket, key);
                if uri.starts_with(location) {
                    results.push(record);
                }
            }
        }
    }
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    /*
     * Return the loaded S3Event from the test fixture in tests/
     */
    fn load_test_event() -> Result<S3Event, anyhow::Error> {
        use std::fs::File;
        use std::io::BufReader;

        let reader = BufReader::new(File::open("tests/s3-delete.json")?);
        let event: S3Event = serde_json::from_reader(reader)?;
        Ok(event)
    }

    #[test]
    fn validated_deletes_no_match() -> Result<(), anyhow::Error> {
        let event = load_test_event()?;
        let location = "s3://example-bucket/tables/nonexistent";
        let result = validated_deletes(&location, event);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(0, result.len(), "There should not be any matching deletes");
        Ok(())
    }

    #[test]
    fn validated_deletes_match() -> Result<(), anyhow::Error> {
        let event = load_test_event()?;
        let location = "s3://example-bucket/tables/test";
        let result = validated_deletes(&location, event);
        assert!(result.is_ok());
        let result = result.unwrap();
        assert_eq!(1, result.len(), "There should be one matching delete");
        Ok(())
    }

    #[test]
    fn remove_actions_for_with_valid() -> Result<(), anyhow::Error> {
        let event = load_test_event()?;
        let location = "s3://example-bucket/tables/test";
        let result = validated_deletes(&location, event)?;

        let actions = remove_actions_for(&result)?;
        assert_eq!(1, actions.len(), "Should only have one remove action");

        match &actions[0] {
            Action::remove(r) => {
                println!("r: {:?}", r);
                assert_eq!("tables/test/part-abc.parquet", r.path);
            }
            _ => {
                assert!(false, "Found an unexpected action");
            }
        }
        Ok(())
    }
}
