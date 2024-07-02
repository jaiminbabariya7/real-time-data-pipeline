from google.cloud import storage, bigquery
import json

# Initialize GCS client
storage_client = storage.Client()
bucket_name = 'finance_data_project'
bucket = storage_client.bucket(bucket_name)

# Function to upload data to GCS
def upload_to_gcs(data, destination_blob_name):
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_string(data)
    print(f'Uploaded data to {destination_blob_name} in bucket {bucket_name}')

"""# Initialize BigQuery client
bigquery_client = bigquery.Client()
dataset_id = 'finance_data'
table_id = 'transformed_data'
table_ref = bigquery_client.dataset(dataset_id).table(table_id)

# Function to upload data to BigQuery
def upload_to_bigquery(data):
    errors = bigquery_client.insert_rows_json(table_ref, [data])
    if errors:
        print(f'Errors while uploading to BigQuery: {errors}')
    else:
        print('Uploaded data to BigQuery')
"""
if __name__ == '__main__':
    data = {'key': 'value'}
    # Upload to GCS
    upload_to_gcs(json.dumps(data), 'data.json')
    # Upload to BigQuery
    #upload_to_bigquery(data)
