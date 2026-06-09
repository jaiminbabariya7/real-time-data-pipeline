output "pubsub_topic"      { value = google_pubsub_topic.raw_events.name }
output "pubsub_sub"        { value = google_pubsub_subscription.dataflow_sub.name }
output "bq_dataset"        { value = google_bigquery_dataset.streaming_etl.dataset_id }
output "gcs_bucket"        { value = google_storage_bucket.pipeline_bucket.name }
output "dataflow_sa_email" { value = google_service_account.dataflow_sa.email }
