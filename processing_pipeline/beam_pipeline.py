from __future__ import absolute_import

import argparse
import datetime
import json
import logging
import os
from dotenv import load_dotenv
import numpy as np
from collections import Counter

import apache_beam as beam
import apache_beam.transforms.window as window
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from apache_beam.options.pipeline_options import StandardOptions, GoogleCloudOptions, SetupOptions, PipelineOptions
from apache_beam.transforms.util import BatchElements

from google.cloud import language


def predict_sentiment(messages):
    if not isinstance(messages, list):
        messages = [messages]

    lang_client = language.LanguageServiceClient()
    instances = list(map(lambda message: json.loads(message), messages))

    for i, instance in enumerate(instances):
        document = language.types.Document(content=instance['text'], type=language.types.Document.Type.PLAIN_TEXT)
        annotated_text = lang_client.annotate_text(document=document, features={'extract_syntax':True, 'extract_document_sentiment':True})
        #print('>>> sentiment', sentiment)
        instance['sentiment_score'] = annotated_text.document_sentiment.score
        instance['sentiment_magnitude'] = annotated_text.document_sentiment.magnitude
        instance['language'] = annotated_text.language

    logging.info("first message")
    logging.info(instances[0])

    return instances

def analyze_batch(messages):
    if not isinstance(messages, list):
        messages = [messages]

    lang_client = language.LanguageServiceClient()
    instances = list(map(lambda message: json.loads(message), messages))
    result = {}
    total_words = 0
    total_characters = 0
    total_sentiment_score = 0
    top_words = Counter()
    top_langs = Counter()
    for i, instance in enumerate(instances):
        document = language.types.Document(content=instance['text'], type=language.types.Document.Type.PLAIN_TEXT)
        annotated_text = lang_client.annotate_text(document=document, features={'extract_syntax':True, 'extract_document_sentiment':True})
        top_words.update(Counter([t.lemma for t in annotated_text.tokens]))
        top_langs[annotated_text.language] += 1
        total_words += len(instance['text'].split(' '))
        total_characters += len(instance['text'])
        total_sentiment_score += annotated_text.document_sentiment.score
        break

    result['time_stamp'] = instances[-1]['time_stamp']
    result['avg_num_words'] = total_words / len(instances)
    result['avg_num_characters'] = total_characters / len(instances)
    result['avg_sentiment_score'] = total_sentiment_score / len(instances)
    result['top_words'] = ",".join([t[0] for t in top_words.most_common(10)])
    result['top_languages'] = ",".join([t[0] for t in top_langs.most_common(10)])
    result['batch_size'] = len(messages)
    logging.info(">>> batch result")
    logging.info(result)
    return result


def run(argv=None):
    load_dotenv()

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
    
    result = run_pipeline(pipeline_options, known_args)
    result.wait_until_finish()

def run_pipeline(pipeline_options, known_args):
    p = beam.Pipeline(options=pipeline_options)
    lines = p | "read in tweets" >> beam.io.ReadFromPubSub(
        subscription=known_args.input_subscription,
        with_attributes=False
        # id_label="tweet_id" # not for direct runner
    )
    output_tweets = (lines
                     | 'add window key' >> beam.WindowInto(window.FixedWindows(10)) # 10 seconds
                     | 'batch messages' >> BatchElements(min_batch_size=2, max_batch_size=50)
                     | 'predict sentiment' >> beam.FlatMap(lambda messages: predict_sentiment(messages))
                     )
    bq_schema_tweets = json.dumps({
        "fields": [{"name": "id", "type": "STRING"},
                   {"name": "time_stamp", "type": "TIMESTAMP"},
                   {"name": "text", "type": "STRING"},
                   {"name": "username", "type": "STRING"},
                   {"name": "sentiment", "type": "INTEGER"},
                   {"name": "sentiment_score", "type": "FLOAT"},
                   {"name": "sentiment_magnitude", "type": "FLOAT"},
                   {"name": "language", "type": "STRING"},
                   {"name": "n_followers", "type": "INTEGER"}]
    })
    output_tweets | 'write to BQ' >> beam.io.WriteToBigQuery(
        table=os.getenv('BQ_TABLE'),
        dataset=os.getenv('BQ_DATASET'),
        schema=parse_table_schema_from_json(bq_schema_tweets),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        project=os.getenv('GC_PROJECT')
    )
    
    output_batch = (lines
                     | 'add window key 2' >> beam.WindowInto(window.FixedWindows(1 * 60)) # 1 minute
                     | 'batch messages 2' >> BatchElements(min_batch_size=10)
                     | 'analyze in batch' >> beam.Map(lambda messages: analyze_batch(messages))
                     )

    bq_schema_batches = json.dumps({
        "fields": [{"name": "time_stamp", "type": "TIMESTAMP"},
                   {"name": "batch_size", "type": "INTEGER"},
                   {"name": "top_words", "type": "STRING"},
                   {"name": "top_languages", "type": "STRING"},
                   {"name": "avg_num_words", "type": "FLOAT"},
                   {"name": "avg_num_characters", "type": "FLOAT"},
                   {"name": "avg_sentiment_score", "type": "FLOAT"}]
    })
    output_batch | 'write to BQ 2' >> beam.io.WriteToBigQuery(
        table=os.getenv('BQ_TABLE_BATCH'),
        dataset=os.getenv('BQ_DATASET'),
        schema=parse_table_schema_from_json(bq_schema_batches),
        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        project=os.getenv('GC_PROJECT')
    )

    return p.run()


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
