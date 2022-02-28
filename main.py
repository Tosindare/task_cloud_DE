"""
Created on Sat Feb 26 21:36:11 2022

@author: TosinOja
"""
################################################################################
#                                                                              #
#  This script contains a pipeline that loads csv file from GCS bucket,        #
#  performs some transformations and produce an output .jsonl file.            #
#                                                                              #
#  requirements.txt included, it shows the required library/dependency         #
#  file - 'unit_test_input.csv' is essential to execute the unit test #
#                                                                              #
################################################################################

# Import libraries
import apache_beam as beam
from datetime import datetime

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import unittest


# function to change date (timestamp) string to date object
def date_format_object(record):
    record[0] = datetime.strptime(record[0][:-4], '%Y-%m-%d %H:%M:%S').date()
    return record


# function to change date object to short date string
def date_format_string(record):
    record[0] = record[0].strftime('%Y-%m-%d')
    return record


# function filter date with year less than 2010
def filter_date_before_2010(record):
    if int(record[0].strftime('%Y')) >= 2010:
        return record


# function to filter transaction_amount greater than 20.00
def transaction_greaterthan_20(record):
    if float(record[3]) > 20.0:
        return record


# function to create dictionary for json file formatting
class CreateKeyValuePair(beam.DoFn):
    def process(self, record):
        header = ('date', 'total_amount')
        yield dict(zip(header, record))


file_path = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'


# Composite transformation
class MyCompositeTransform(beam.PTransform):
    def expand(self, record):
        data = (
                record
                | 'Split by commas' >> beam.Map(lambda item: item.split(','))
                | 'date to object' >> beam.Map(date_format_object)
                | 'transactions > 20' >> beam.Filter(transaction_greaterthan_20)
                | 'transaction dates > 2010' >> beam.Filter(filter_date_before_2010)
                | 'date to string' >> beam.Map(date_format_string)
                | 'Transaction date and amount' >> beam.Map(lambda item: (item[0], float(item[3])))
                | 'Group and sum' >> beam.CombinePerKey(sum)
                | 'Create key_value pair' >> beam.ParDo(CreateKeyValuePair())

        )

        return data


# Sequential pipeline

# def main():
#     with beam.Pipeline() as pipe:
#         tasks = (
#
#                 pipe
#                 | 'Read csv file' >> beam.io.ReadFromText(file_path, skip_header_lines=1)
#                 | 'Split by commas' >> beam.Map(lambda record: record.split(','))
#                 | 'date to object' >> beam.Map(date_format_object)
#                 | 'transactions > 20' >> beam.Filter(transaction_greaterthan_20)
#                 | 'transaction dates > 2010' >> beam.Filter(filter_date_before_2010)
#                 | 'date to string' >> beam.Map(date_format_string)
#                 | 'Transaction date and amount' >> beam.Map(lambda record: (record[0], float(record[3])))
#                 | 'Group and sum' >> beam.CombinePerKey(sum)
#                 | 'Create key_value pair' >> beam.ParDo(CreateKeyValuePair())
#                 | 'Output file' >> beam.io.WriteToText('output/results.jsonl.gz', shard_name_template='')
#
#         )

# Pipeline with Composite transformation
def main_composite():
    with beam.Pipeline() as pipe:
        tasks = (
                pipe
                | 'Read csv file' >> beam.io.ReadFromText(file_path, skip_header_lines=1)
                | 'Composite transformation' >> MyCompositeTransform()
                | 'output file' >> beam.io.WriteToText('output/results.jsonl.gz', shard_name_template='')

        )


# Unit testing for Composite transformation
class MyCompositeTransformTest(unittest.TestCase):

    def test_composite(self):
        # Input data 'unit_test_input.csv' is used to build Pcollection for test

        EXPECTED_OUTPUT = [
            {'date': '2017-03-18', 'total_amount': 2102.22}
        ]

        # Create a test pipeline.
        with TestPipeline() as p:
            # Create an input PCollection.
            input = p | beam.io.ReadFromText('unit_test_input.csv')

            # Apply the Composite transform under test.
            output = input | MyCompositeTransform()

            # Assert on the results.
            assert_that(output, equal_to(EXPECTED_OUTPUT), label='CheckOutput data')


# uncomment to run unit test
if __name__ == '__main__':
    # unittest.main()
    main_composite()
    # main()
