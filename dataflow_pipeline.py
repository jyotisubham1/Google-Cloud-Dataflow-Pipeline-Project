import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import json
import logging

# Define pipeline options
class MyOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add arguments for input Pub/Sub subscription and output BigQuery table
        parser.add_value_provider_argument('--input_subscription', type=str, help='Pub/Sub subscription to read from')
        parser.add_value_provider_argument('--output_table', type=str, help='BigQuery table to write to')

# Function to parse JSON messages from Pub/Sub
def parse_json(message):
    try:
        # Attempt to parse the message as JSON
        return json.loads(message.decode('utf-8'))
    except json.JSONDecodeError as e:
        # Log an error if JSON parsing fails
        logging.error(f'Error parsing JSON: {e}')
        return None

# Function to format the parsed data
def format_data(elem):
    if elem is None:
        return None
    try:
        # Return a dictionary with the required fields, providing None for missing fields
        return {
            'user_id': elem['user_id'],
            'event_type': elem['event_type'],
            'event_timestamp': elem['event_timestamp'],
            'event_details': elem.get('event_details', None)
        }
    except KeyError as e:
        # Log an error if expected fields are missing
        logging.error(f'Missing expected field: {e}')
        return None

# Define the main pipeline
def run():
    # Initialize pipeline options including Google Cloud Dataflow options
    options = PipelineOptions()
    google_cloud_options = options.view_as(GoogleCloudOptions)
    google_cloud_options.project = 'data-processing-systems'  # Replace with your Google Cloud project ID
    google_cloud_options.temp_location = 'gs://my-dataflow-bucket-12/tmp/'  # Replace with your GCS temp location
    google_cloud_options.staging_location = 'gs://my-dataflow-bucket-12/staging/'  # Replace with your GCS staging location
    google_cloud_options.region = 'us-central1'  # Replace with your preferred Google Cloud region
    options.view_as(StandardOptions).runner = 'DataflowRunner'  # Set the runner to DataflowRunner
    options.view_as(StandardOptions).streaming = True  # Enable streaming mode

    # Hardcoded values for testing
    subscription = "projects/data-processing-systems/subscriptions/my-data-subscription"
    output_table = "data-processing-systems:my_dataset.my_table"

    # Define the BigQuery table schema
    table_schema = {
        'fields': [
            {'name': 'user_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'event_type', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'event_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
            {'name': 'event_details', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
    }

    # Create the pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'Read from Pub/Sub' >> ReadFromPubSub(subscription=subscription)  # Read messages from Pub/Sub
            | 'Parse JSON' >> beam.Map(parse_json)  # Parse JSON messages
            | 'Filter None Values' >> beam.Filter(lambda x: x is not None)  # Filter out None values from parsing errors
            | 'Format Data' >> beam.Map(format_data)  # Format the parsed data
            | 'Filter Formatted None Values' >> beam.Filter(lambda x: x is not None)  # Filter out None values from formatting errors
            | 'Write to BigQuery' >> WriteToBigQuery(
                output_table,
                schema=table_schema,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )  # Write data to BigQuery
        )

# Entry point for the script
if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)  # Set logging level to INFO
    run()  # Run the pipeline
