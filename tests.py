import unittest
import hashlib
import apache_beam as beam
from apache_beam.testing.util import assert_that, equal_to
from apache_beam.testing.test_pipeline import TestPipeline
from main import transaction_by_property, group_to_json, generate_property_id, CombineTransactionsFn
import json

class TestPipelineFunctions(unittest.TestCase):
    def test_generate_property_id(self):
        transaction = {
            'Postcode': '12345',
            'PAON': '10'
        }
        expected_property_id = hashlib.sha256('12345-10'.encode()).hexdigest()
        self.assertEqual(generate_property_id(transaction), expected_property_id)

    def test_group_to_json(self):
        property_transactions = (
            'property_id', [
                {
                    'TransactionID': 'txn1',
                    'Price': '100000',
                    'Date': '2023-01-01',
                    'Postcode': '12345',
                    'PropertyType': 'D',
                    'OldNew': 'N',
                    'Duration': 'F',
                    'PAON': '10',
                    'SAON': '',
                    'Street': 'Main Street',
                    'Locality': '',
                    'TownCity': 'London',
                    'District': 'District A',
                    'County': 'County B',
                    'PPDCategoryType': 'A',
                    'RecordStatus': 'A'
                }
            ]
        )
        json_output = group_to_json(property_transactions)
        json_data = json.loads(json_output)
        self.assertEqual(json_data['PropertyID'], 'property_id')
        self.assertEqual(len(json_data['Transactions']), 1)
        self.assertEqual(json_data['Transactions'][0]['TransactionID'], 'txn1')

    def test_end_to_end_pipeline(self):
        test_input = [
            {
                'TransactionID': 'txn1',
                'Price': '100000',
                'Date': '2023-01-01',
                'Postcode': '12345',
                'PropertyType': 'D',
                'OldNew': 'N',
                'Duration': 'F',
                'PAON': '10',
                'SAON': '',
                'Street': 'Main Street',
                'Locality': '',
                'TownCity': 'London',
                'District': 'District A',
                'County': 'County B',
                'PPDCategoryType': 'A',
                'RecordStatus': 'A'
            },
            {
                'TransactionID': 'txn2',
                'Price': '150000',
                'Date': '2023-01-05',
                'Postcode': '12345',
                'PropertyType': 'D',
                'OldNew': 'N',
                'Duration': 'F',
                'PAON': '10',
                'SAON': '',
                'Street': 'Main Street',
                'Locality': '',
                'TownCity': 'London',
                'District': 'District A',
                'County': 'County B',
                'PPDCategoryType': 'A',
                'RecordStatus': 'A'
            }
        ]

        # Convert expected output to deserialized Python objects
        expected_output = [
            {
                'PropertyID': generate_property_id(test_input[0]),
                'PAON': '10',
                'SAON': '',
                'Street': 'Main Street',
                'Locality': '',
                'TownCity': 'London',
                'District': 'District A',
                'County': 'County B',
                'Postcode': '12345',
                'PropertyType': 'D',
                'Transactions': [
                    {
                        'TransactionID': 'txn1',
                        'Price': '100000',
                        'Date': '2023-01-01',
                        'OldNew': 'N',
                        'Duration': 'F',
                        'PPDCategoryType': 'A',
                        'RecordStatus': 'A',
                    },
                    {
                        'TransactionID': 'txn2',
                        'Price': '150000',
                        'Date': '2023-01-05',
                        'OldNew': 'N',
                        'Duration': 'F',
                        'PPDCategoryType': 'A',
                        'RecordStatus': 'A',
                    }
                ]
            }
        ]

        with TestPipeline() as p:
            input_data = p | 'Create Test Data' >> beam.Create(test_input)

            output = (
                    input_data
                    | 'Key by Property' >> beam.Map(transaction_by_property)
                    | 'Combine Transactions' >> beam.CombinePerKey(CombineTransactionsFn())
                    | 'Convert to JSON' >> beam.Map(lambda x: json.loads(group_to_json(x)))  # Deserialize JSON
            )

            # Compare the deserialized objects, not JSON strings
            assert_that(output, equal_to(expected_output))

if __name__ == '__main__':
    unittest.main()
