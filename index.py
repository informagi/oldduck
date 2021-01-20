import argparse
import duckdb

from pyserini import collection, index


class Index:

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
            'collection': None
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['database'] is None:
            raise IOError('Database path needs to be provided.')
        if arguments['collection'] is None:
            raise IOError('Collection path needs to be provided.')
        return arguments

    def create_input_table(self):
        self.cursor.execute(f"CREATE TABLE documents(id VARCHAR, body VARCHAR)")
        self.connection.begin()
        c = collection.Collection('TrecCollection', self.arguments['collection'])
        generator = index.Generator('DefaultLuceneDocumentGenerator')
        for fs in c:
            for i, doc in enumerate(fs):
                if i % 10000 == 0:
                    self.connection.commit()
                    self.connection.begin()
                try:
                    parsed = generator.create_document(doc)
                except:
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
                        '--collection',
                        required=True,
                        metavar='[directory]',
                        help='Location of the collection.')
    Index(**vars(parser.parse_args()))
