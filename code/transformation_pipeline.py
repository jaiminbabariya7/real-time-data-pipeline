import argparse
import json
import logging
from datetime import datetime
import random

import apache_beam as beam
from apache_beam import DoFn, ParDo, Pipeline, PTransform, WindowInto, WithKeys
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions, SetupOptions, GoogleCloudOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from schema_definition import schema


class GroupMessagesByFixedWindows(PTransform):
    def __init__(self, window_size, num_shards=5):
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            | "Window into fixed intervals" >> WindowInto(FixedWindows(self.window_size))
            | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            | "Group by key" >> beam.GroupByKey()
        )


class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime("%Y-%m-%d %H:%M:%S.%f"),
        )


class AddColumn(DoFn):
    def process(self, element):
        def process(self, element):
            try:
                row = json.loads(element[0])  # element[0] contains the message body
                logging.info(f"Row after JSON parsing: {row}")

                # Merge 'purpose' and 'duration' to create 'new_column'
                purpose = row.get('purpose', '')
                duration = row.get('duration', '')
                row['new_column'] = f"{purpose}-{duration}"
            
                return [row]
            
            except Exception as e:
                logging.error(f"Error processing element: {element}, Error: {e}")
                return []


def run(output_table='ecommerce_dataset.finance_data', window_size=1.0, num_shards=5, pipeline_args=None):
    project_id = 'finalproject10071998'
    input_subscription = f'projects/{project_id}/subscriptions/finance_data-sub'
    
    pipeline_options = PipelineOptions(pipeline_args, streaming=True, save_main_session=True)
    gcloud_options = pipeline_options.view_as(GoogleCloudOptions)
    gcloud_options.project = project_id
    gcloud_options.job_name = 'pubsub-to-bigquery'
    gcloud_options.temp_location = 'gs://finance_data_project/temp'
    gcloud_options.region = 'us-central1' #'northamerica-northeast2'
    pipeline_options.view_as(StandardOptions).runner = 'DataflowRunner'
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Read from Pub/Sub" >> ReadFromPubSub(subscription=input_subscription)
            | "Window into" >> GroupMessagesByFixedWindows(window_size, num_shards)
            | "Add Column" >> ParDo(AddColumn())
            | "Write to BigQuery" >> WriteToBigQuery(
                output_table,
                schema=schema,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output_table",
        default='ecommerce_dataset.finance_data',
        help="The BigQuery table to write to. 'your_dataset.your_table'.",
    )
    parser.add_argument(
        "--window_size",
        type=float,
        default=1.0,
        help="Output file's window size in minutes.",
    )
    parser.add_argument(
        "--num_shards",
        type=int,
        default=5,
        help="Number of shards to use when grouping windowed elements.",
    )
    known_args, pipeline_args = parser.parse_known_args()

    logging.info(f"Output Table: {known_args.output_table}")
    logging.info(f"Window Size: {known_args.window_size}")
    logging.info(f"Num Shards: {known_args.num_shards}")

    run(
        known_args.output_table,
        known_args.window_size,
        known_args.num_shards,
        pipeline_args,
    )

