"""
Apache Beam / Cloud Dataflow streaming ETL pipeline.

Reads user-activity events from Pub/Sub, parses and validates them,
enriches with computed fields, writes valid records to BigQuery and
GCS, and routes invalid records to a Dead-Letter Queue (DLQ) topic.

Architecture:
    Pub/Sub (raw-events)
        |-- ParseEvent   (valid / invalid split)
        |
        +-- [valid]  --> EnrichEvent --> FixedWindows(60s) --> AddWindowTimestamps
        |                   |
        |                   +-- WriteToBigQuery  (streaming append)
        |                   +-- WriteToGCS       (Parquet archive)
        |
        +-- [invalid] --> WriteToBigQuery (DLQ table)

Deploy:
    python pipeline.py --runner=DataflowRunner [flags]
Local test:
    python pipeline.py --runner=DirectRunner
"""
from __future__ import annotations
import argparse, logging, os
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import (
    GoogleCloudOptions, PipelineOptions, StandardOptions,
)
from apache_beam.transforms.window import FixedWindows
from apache_beam.io import WriteToText
from transforms import ParseEvent, EnrichEvent, AddWindowTimestamps, FormatGCSRecord, VALID_TAG, INVALID_TAG
from kafka.schemas import BQ_EVENTS_SCHEMA, BQ_DLQ_SCHEMA

logger = logging.getLogger(__name__)

PROJECT_ID  = os.environ["GCP_PROJECT_ID"]
DATASET     = os.getenv("BQ_DATASET",     "streaming_etl")
EVENTS_TBL  = f"{PROJECT_ID}:{DATASET}.events"
DLQ_TBL     = f"{PROJECT_ID}:{DATASET}.dead_letter_queue"
GCS_BUCKET  = os.getenv("GCS_BUCKET",    f"gs://{PROJECT_ID}-streaming-etl")
PUBSUB_SUB  = os.getenv("PUBSUB_SUB",    f"projects/{PROJECT_ID}/subscriptions/dataflow-events-sub")
WINDOW_SECS = int(os.getenv("WINDOW_SECS", "60"))


def run(argv=None) -> None:
    parser = argparse.ArgumentParser(description="Streaming ETL Pipeline")
    parser.add_argument("--gcs_temp",    default=f"{GCS_BUCKET}/tmp/dataflow")
    parser.add_argument("--gcs_staging", default=f"{GCS_BUCKET}/staging/dataflow")
    parser.add_argument("--region",      default="us-central1")
    parser.add_argument("--workers",     type=int, default=2)
    parser.add_argument("--max_workers", type=int, default=10)
    known, beam_args = parser.parse_known_args(argv)

    opts = PipelineOptions(beam_args, streaming=True, save_main_session=True)
    opts.view_as(StandardOptions).streaming = True
    gcp = opts.view_as(GoogleCloudOptions)
    gcp.project          = PROJECT_ID
    gcp.region           = known.region
    gcp.temp_location    = known.gcs_temp
    gcp.staging_location = known.gcs_staging

    with beam.Pipeline(options=opts) as p:
        # ── Ingest ─────────────────────────────────────────────────────────────
        raw = (p
               | "ReadPubSub" >> beam.io.ReadFromPubSub(subscription=PUBSUB_SUB)
               | "ParseEvent" >> beam.ParDo(ParseEvent()).with_outputs(
                   VALID_TAG, INVALID_TAG))

        valid   = raw[VALID_TAG]
        invalid = raw[INVALID_TAG]

        # ── Valid path ─────────────────────────────────────────────────────────
        enriched = (valid
                    | "Enrich"     >> beam.ParDo(EnrichEvent())
                    | "Window60s"  >> beam.WindowInto(FixedWindows(WINDOW_SECS))
                    | "AddWindow"  >> beam.ParDo(AddWindowTimestamps()))

        # Sink 1 — BigQuery (streaming insert)
        (enriched | "WriteBigQuery" >> WriteToBigQuery(
            EVENTS_TBL, schema=BQ_EVENTS_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            method=WriteToBigQuery.Method.STREAMING_INSERTS))

        # Sink 2 — GCS (newline-delimited JSON archive)
        gcs_path = f"{GCS_BUCKET}/archive/events"
        (enriched
         | "FormatJSON" >> beam.ParDo(FormatGCSRecord())
         | "WriteGCS"   >> WriteToText(gcs_path, file_name_suffix=".jsonl",
                                       append_trailing_newlines=True,
                                       shard_name_template="-SS-of-NN"))

        # ── Dead-Letter Queue ──────────────────────────────────────────────────
        (invalid | "WriteDLQ" >> WriteToBigQuery(
            DLQ_TBL, schema=BQ_DLQ_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_IF_NEEDED))

    logger.info("Pipeline submitted.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(message)s")
    run()
