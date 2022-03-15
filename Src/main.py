from pymongo import MongoClient
from pprint import pprint


def extract_random_records(collection, nb_records=10, conditions=None):
    if conditions is None:
        pipeline = []
    else:
        pipeline = [{'$match': conditions}]
    pipeline.append({'$sample': {'size': nb_records}})
    records = list(collection.aggregate(pipeline))

    return records


if __name__ == '__main__':
    # Connection to the collection of suppliers
    with open('mongoDB_credentials.txt', 'r') as f:
        username = f.readline().strip()
        pwd = f.readline().strip()
    connection_str = f'mongodb+srv://{username}:{pwd}@hackbridge.hdjhe.mongodb.net/' \
                     f'hackbridge?retryWrites=true&w=majority'

    client = MongoClient(connection_str)
    database = client['hackbridge']
    collection = database['supplier_database']

    # A few examples on how we can make requests on this collection
    one_record = collection.find_one()  # Sample one supplier from the collection
    pprint(one_record)  # Pretty-print the sampled supplier, it consists in a (nested) Python dictionary

    # If you want to sample multiple/all the suppliers, you can make use of the find method
    records = collection.find()  # Please note that this will not return a list but a generator
    for record in records[:2]:  # Limit to the first two records
        pprint(record)

    # These former methods are deterministic, i.e. they will always return you the same records
    # If you want different records each time, you can make use of the following method:
    records = extract_random_records(collection, nb_records=1)
    pprint(records)

    # You can also filter the collection in order to get only the records that match given conditions. The conditions
    # should be given with a Python dictionary:
    conditions = {'origin': 'Organicbio'}  # e.g. we can limit to suppliers coming from the Organicbio directory
    print('\nOne supplier coming from the Organicbio directory: ')
    pprint(collection.find_one(conditions))

    # You can specify multiple conditions that will need to be satisfied at the same time
    print('\nOne UK or US-based supplier coming from the Organicbio directory: ')
    conditions['postalAddress.addressCountry'] = {'$in': ['United Kingdom', 'United States']}
    pprint(extract_random_records(collection, nb_records=1, conditions=conditions))

    # You can list all the unique values of a field that are in the collection with:
    print('\nAll the web directories that we used to populate our supplier collection:')
    pprint(collection.distinct('origin'))

    # Finally, you can easily count the number of documents in the collection with:
    conditions = {'keywords': {'$in': ['fruit']}}
    nb = collection.count_documents(conditions)
    print(f'\nNumber of suppliers whose keywords include the term "fruit": {nb}')

    # Do not hesitate to contact us if you need more advanced usage, e.g. counting the number of occurences
    # of each value
