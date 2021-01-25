from __future__ import absolute_import

import logging
import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import ReadFromPubSub
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from past.builtins import unicode
import sys
import subscriber


def run(argv=None, save_main_session=True):
    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(argv)
    pipeline_options.view_as(SetupOptions).save_main_session = save_main_session
    with beam.Pipeline(options=pipeline_options) as p:
        # Read the text file[pattern] into a PCollection.
        # input_text = '*.txt'
        output_text = 'output_test.txt'
        # lines = p | ReadFromText(input_text)
        lines = p | beam.Create(subscriber.receive_message())

        # Count the occurrences of each word.
        counts = (
                lines
                | 'Split' >> (
                    beam.FlatMap(lambda x: re.findall(r'(?i)[a-ząćęłńóśźż\']+', x)).
                        with_output_types(unicode))
                | 'PairWithOne' >> beam.Map(lambda x: (x, 1))
                | 'GroupAndSum' >> beam.CombinePerKey(sum))

        # Format the counts into a PCollection of strings.
        def format_result(word_count):
            (word, count) = word_count
            return '%s: %s' % (word, count)

        output = counts | 'Format' >> beam.Map(format_result)

        # Write the output using a "Write" transform that has side effects.
        # pylint: disable=expression-not-assigned
        output | WriteToText(output_text)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run(sys.argv)
