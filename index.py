import argparse
import duckdb

from pyserini.search import SimpleSearcher


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
            'index': None
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['database'] is None:
            raise IOError('Database path needs to be provided.')
        if arguments['index'] is None:
            raise IOError('Collection path needs to be provided.')
        return arguments

    def create_input_table(self):
        searcher = SimpleSearcher(self.arguments['index'])
        self.cursor.execute(f"CREATE TABLE documents(id VARCHAR, body VARCHAR)")
        self.connection.begin()
        for i in range(searcher.num_docs):
            if i % 10000 == 0:
                self.connection.commit()
                self.connection.begin()
            doc = searcher.doc(i)
            self.cursor.execute(f"INSERT INTO documents VALUES (?, ?)",
                                (doc.docid(), doc.contents())
                                )
        self.connection.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        '--database',
                        required=True,
                        metavar='[file]',
                        help='Location of the database.')
    parser.add_argument('-i',
                        '--index',
                        required=True,
                        metavar='[directory]',
                        help='Location of the anserini index.')
    Index(**vars(parser.parse_args()))
