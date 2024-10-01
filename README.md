# Property Transactions Apache Beam Pipeline
This project is an Apache Beam pipeline for processing property transaction data, grouping transactions by property, and transforming the data into a JSON format. The pipeline reads property transactions from a CSV file, processes them by creating a unique PropertyID, groups them by propertyID, and outputs the results in JSON format.

## Features

1. Reading CSV: CSV is read line by line using generator to make the code memory efficient.
2. Property ID: is generated using hashed combination of postcode and PAON.
3. Grouping by Property: Transactions are grouped by properties using property_id.
4. Custom Combine Function: A custom combiner is used to efficiently merge transactions for the same property.  There is no groupbykey function to make the pipeline more scalable and memory efficient.
5. JSON Output: The final output is structured as JSON for easy integration with downstream systems.

## Running
1. Make sure you have apache beam installed.
2. Run main.py to see the output.

## Testing
The tests.py file contains unit tests to verify the correctness of the pipeline. It uses the Apache Beam testing framework. There are 3 unit test cases within this function.
