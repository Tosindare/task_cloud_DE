"""
Created on Sat Feb 26 21:36:11 2022

@author: TosinOja
"""

import apache_beam as beam
from datetime import datetime


def date_format_object(record):
    record[0] = datetime.strptime(record[0][:-4], '%Y-%m-%d %H:%M:%S').date()
    return record


def date_format_string(record):
    record[0] = record[0].strftime('%Y-%m-%d')
    return record


def filter_date_before_2010(record):
    if int(record[0].strftime('%Y')) >= 2010:
        return record


def transaction_greaterthan_20(record):
    if float(record[3]) > 20.0:
        return record


class CreateKeyValuePair(beam.DoFn):
    def process(self, record):
        header = ('date', 'total_amount')
        yield dict(zip(header, record))


file_path = 'gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv'


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
#                 # | beam.Map(print)
#                 | 'Output file' >> beam.io.WriteToText('output/results.jsonl', shard_name_template='')
#
#         )


def main_composite():
    with beam.Pipeline() as pipe:
        tasks = (
                pipe
                | 'Read csv file' >> beam.io.ReadFromText(file_path, skip_header_lines=1)
                | 'Composite transformation' >> MyCompositeTransform()
                | 'output file' >> beam.io.WriteToText('output/results.jsonl', shard_name_template='')

        )


if __name__ == '__main__':
    main_composite()
    # main()
