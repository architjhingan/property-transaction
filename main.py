import apache_beam as beam
import codecs
import csv
import json
import hashlib
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions


def read_csv_file(readable_file):
    with beam.io.filesystems.FileSystems.open(readable_file) as file:
        column_names = [
            'TransactionID', 'Price', 'Date', 'Postcode',
            'PropertyType', 'OldNew', 'Duration', 'PAON',
            'SAON', 'Street', 'Locality', 'TownCity',
            'District', 'County', 'PPDCategoryType', 'RecordStatus'
        ]
        for row in csv.DictReader(codecs.iterdecode(file, 'utf-8'), fieldnames=column_names):
            print(row)
            yield row


def generate_property_id(transaction):
    # We could use a combination of address, city, and zip code as a unique property ID
    address = f"{transaction['Postcode']}-{transaction['PAON']}"
    property_id = hashlib.sha256(address.encode()).hexdigest()
    return property_id


def transaction_by_property(transaction):
    property_id = generate_property_id(transaction)
    return (property_id, transaction)


def group_to_json(property_transactions):
    property_id, transactions = property_transactions
    first_transaction = transactions[0]

    property_object = {
        'PropertyID': property_id,
        'PAON': first_transaction['PAON'],
        'SAON': first_transaction['SAON'],
        'Street': first_transaction['Street'],
        'Locality': first_transaction['Locality'],
        'TownCity': first_transaction['TownCity'],
        'District': first_transaction['District'],
        'County': first_transaction['County'],
        'Postcode': first_transaction['Postcode'],
        'PropertyType': first_transaction['PropertyType'],
        'Transactions': [
            {
                'TransactionID': transaction['TransactionID'],
                'Price': transaction['Price'],
                'Date': transaction['Date'],
                'OldNew': transaction['OldNew'],
                'Duration': transaction['Duration'],
                'PPDCategoryType': transaction['PPDCategoryType'],
                'RecordStatus': transaction['RecordStatus'],
            }
            for transaction in transactions
        ]
    }
    return json.dumps(property_object)


class CombineTransactionsFn(beam.CombineFn):
    def create_accumulator(self):
        return []

    def add_input(self, accumulator, transaction):
        accumulator.append(transaction)
        return accumulator

    def merge_accumulators(self, accumulators):
        result = []
        for acc in accumulators:
            result.extend(acc)
        return result

    def extract_output(self, accumulator):
        return accumulator


def run_pipeline(input_file, output_file):
    options = PipelineOptions(
        runner='DirectRunner'
    )

    with beam.Pipeline(options=options) as p:
        (
                p
                | 'Read CSV' >> beam.Create([input_file])
                | 'Flatten the CSV' >> beam.FlatMap(read_csv_file)
                | 'Key by Property' >> beam.Map(transaction_by_property)
                | 'Combine Transactions' >> beam.CombinePerKey(CombineTransactionsFn())
                | 'Convert to JSON' >> beam.Map(group_to_json)
                | 'Write JSON' >> WriteToText(output_file, file_name_suffix='.json')
        )

# Run the pipeline with input and output files
if __name__ == '__main__':
    input_file = 'pp-monthly.csv'
    output_file = 'transformed'
    run_pipeline(input_file, output_file)

