from __future__ import absolute_import

import argparse
import datetime
import json
import logging
import os
from dotenv import load_dotenv
import numpy as np

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, SetupOptions, PipelineOptions
from apache_beam.transforms.util import BatchElements


def predict_sentiment(messages):
    if not isinstance(messages, list):
        messages = [messages]

    instances = list(map(lambda message: json.loads(message), messages))

    scores = [1 for instance in instances]

    for i, instance in enumerate(instances):
        instance['sentiment'] = scores[i]

    logging.info("first message")
    logging.info(instances[0])

    return instances


def run(argv=None):
    load_dotenv()

    bigqueryschema_json = json.dumps({
        "fields": [{"name": "id", "type": "STRING"},
                   {"name": "time_stamp", "type": "TIMESTAMP"},
                   {"name": "text", "type": "STRING"},
                   {"name": "username", "type": "STRING"},
                   {"name": "sentiment", "type": "INTEGER"},
                   {"name": "n_followers", "type": "INTEGER"}]
    })
    bigqueryschema = parse_table_schema_from_json(bigqueryschema_json)

    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        '--input_subscription',
        help=('Input PubSub subscription of the form '
              '"projects/<PROJECT>/subscriptions/<SUBSCRIPTION>."'),
        default=os.getenv('PUBSUB_SUBSCRIPTION_FULL')

    )
    group.add_argument(
        '--input_topic',
        help=('Input PubSub topic of the form '
              '"projects/<PROJECT>/topics/<TOPIC>."'),
        default=os.getenv('PUBSUB_TOPIC_FULL')
    )

    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(StandardOptions).streaming = True
    pipeline_options.view_as(StandardOptions).runner = 'DirectRunner'

    p = beam.Pipeline(options=pipeline_options)
    lines = p | "read in tweets" >> beam.io.ReadFromPubSub(
        subscription=known_args.input_subscription,
        with_attributes=False
        # id_label="tweet_id" # not for direct runner
    )
    output_tweets = (lines
                     | 'add window key' >> beam.WindowInto(window.FixedWindows(10))
                     | 'batch messages' >> BatchElements(min_batch_size=2, max_batch_size=50)
                     | 'predict sentiment' >> beam.FlatMap(lambda messages: predict_sentiment(messages))
                     )
    output_tweets | 'write to BQ' >> beam.io.WriteToBigQuery(
        table=os.getenv('BQ_TABLE'),
        dataset=os.getenv('BQ_DATASET'),
        schema=bigqueryschema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        project=os.getenv('GC_PROJECT')
    )
    result = p.run()
    result.wait_until_finish()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
