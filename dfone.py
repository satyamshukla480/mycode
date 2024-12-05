import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery
import argparse

# Define the schema for the BigQuery destination table
table_schema = {
    "fields": [
        {"name": "column1", "type": "STRING", "mode": "REQUIRED"},
        {"name": "column2", "type": "INTEGER", "mode": "NULLABLE"},
    ]
}

def run(argv=None):
    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Input BigQuery table in the format PROJECT:DATASET.TABLE')
    parser.add_argument('--output', dest='output', required=True, help='Output BigQuery table in the format PROJECT:DATASET.TABLE')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set pipeline options
    options = PipelineOptions(pipeline_args)
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'YOUR_PROJECT_ID'  # Replace with your GCP project ID
    google_cloud_options.job_name = 'bq-to-bq-dataflow-job'  # Unique job name
    google_cloud_options.staging_location = 'gs://YOUR_BUCKET/staging'  # Replace with your GCS bucket for staging
    google_cloud_options.temp_location = 'gs://YOUR_BUCKET/temp'  # Replace with your GCS bucket for temp files
    options.view_as(StandardOptions).runner = 'DataflowRunner'  # Using Dataflow Runner

    # Create the pipeline
    with beam.Pipeline(options=options) as p:
        # Read data from the source BigQuery table
        rows = p | 'ReadFromBigQuery' >> ReadFromBigQuery(table=known_args.input)

        # Transform the data if necessary (e.g., selecting specific columns)
        def transform_row(row):
            return {
                'column1': row['column1'],
                'column2': row['column2']
            }

        transformed_rows = rows | 'TransformRows' >> beam.Map(transform_row)

        # Write data to the destination BigQuery table
        transformed_rows | 'WriteToBigQuery' >> WriteToBigQuery(
            known_args.output,
            schema=table_schema,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    run()
