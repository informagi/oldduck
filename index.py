import argparse
import duckdb

from pyserini import collection, index


class Index:

    _col_name_col_generator_map = {
        'robust04': 'DefaultLuceneDocumentGenerator',
        'core17': 'DefaultLuceneDocumentGenerator',
        'core18': 'WashingtonPostGenerator'
    }

    _col_name_col_type_map = {
        'robust04': 'TrecCollection',
        'core17': 'NewYorkTimesCollection',
        'core18': 'WashingtonPostCollection'
    }

    def __init__(self, **kwargs):
        self.arguments = self.get_arguments(kwargs)
        self.connection = duckdb.connect(self.arguments['database'])
        self.cursor = self.connection.cursor()

        self.create_input_table()
        self.cursor.execute("PRAGMA create_fts_index('documents', 'id', '*')")

    @staticmethod
    def get_arguments(kwargs):
        arguments = {
            'database': None,
            'collection_location': None,
            'collection_name': None
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['database'] is None:
            raise IOError('Database path needs to be provided.')
        if arguments['collection_location'] is None:
            raise IOError('Collection path needs to be provided.')
        if arguments['collection_name'] is None:
            raise IOError('Collection name needs to be provided.')
        if arguments['collection_name'] not in ['robust04', 'core17', 'core18']:
            raise IOError('Collection name needs to be one of: robust04, core17, core18')
        return arguments

    def create_input_table(self):
        self.cursor.execute(f"CREATE TABLE documents(id VARCHAR, body VARCHAR)")
        self.connection.begin()
        c = collection.Collection(self._col_name_col_type_map[self.arguments['collection_name']],
                                  self.arguments['collection_location'])
        generator = index.Generator(self._col_name_col_generator_map[self.arguments['collection_name']])
        for fs in c:
            for i, doc in enumerate(fs):
                if i % 10000 == 0:
                    self.connection.commit()
                    self.connection.begin()
                try:
                    parsed = generator.create_document(doc)
                except:  # The document is empty
                    pass
                doc_id = parsed.get("id")
                contents = parsed.get("contents")
                self.cursor.execute(f"INSERT INTO documents VALUES (?, ?)",
                                    (doc_id, contents)
                                    )
        self.connection.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        '--database',
                        required=True,
                        metavar='[file]',
                        help='Location of the database.')
    parser.add_argument('-c',
                        '--collection_location',
                        required=True,
                        metavar='[directory]',
                        help='Location of the collection.')
    parser.add_argument('-n',
                        '--collection_name',
                        required=True,
                        metavar=['string'],
                        choices=['robust04', 'core17', 'core18'],
                        help='Name of the collection')
    Index(**vars(parser.parse_args()))
