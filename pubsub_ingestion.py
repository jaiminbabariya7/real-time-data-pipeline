from google.cloud import pubsub_v1, storage
import json, csv, time, sys

# Initialize a Publisher client
publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/finalproject10071998/topics/finance_data'

# Initialize a Subscriber client
subscriber = pubsub_v1.SubscriberClient()
subscription_path = 'projects/finalproject10071998/subscriptions/finance_data-sub'

# Initialize GCS client
storage_client = storage.Client()
bucket_name = 'finance_data_project'
bucket = storage_client.bucket(bucket_name)

# Function to publish messages to Pub/Sub
def publish_messages(data):
    message = json.dumps(data).encode('utf-8')
    future = publisher.publish(topic_path, data=message)
    print(f'Published message ID: {future.result()}')

# Function to read CSV from GCS and publish messages to Pub/Sub
def read_csv_and_publish():
    blob = bucket.blob('data/Finance_data.csv')
    csv_data = blob.download_as_string().decode('utf-8')
    csv_reader = csv.DictReader(csv_data.splitlines())

    for row in csv_reader:
        publish_messages(row)

"""
# Function to receive messages from Pub/Sub
def callback(message):
    try:
        print(f'Received message: {message.data}')

    except Exception as e:
        print(f'Error processing message: {e}')
    
    message.ack()

def receive_messages():
    with subscriber:
        streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
        print(f'Listening for messages on {subscription_path}...')

        try:
            streaming_pull_future.result(timeout=2)
        
        except KeyboardInterrupt:
            print('Received keyboard interrrupt. Stopping...')
            streaming_pull_future.cancel()
            sys.exit()

        except Exception as e:
            streaming_pull_future.cancel()
            sys.exit()
"""

if __name__ == '__main__':
    read_csv_and_publish()
    #receive_messages()
