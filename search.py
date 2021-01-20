import argparse
import duckdb

from pyserini.search import get_topics


class Search:

    def __init__(self, **kwargs):
        self.arguments = self.get_arguments(kwargs)
        self.connection = duckdb.connect(self.arguments['database'])
        self.cursor = self.connection.cursor()

        self.prepare_bm25()
        self.run_topics()

    @staticmethod
    def get_arguments(kwargs):
        arguments = {
            'database': None,
            'collection_name': None,
            'outfile': None
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['database'] is None:
            raise IOError('Database path needs to be provided.')
        if arguments['collection_name'] is None:
            raise IOError('Collection name needs to be provided.')
        if arguments['outfile'] is None:
            raise IOError('Output file needs to be provided.')
        return arguments

    def prepare_bm25(self):
        self.cursor.execute("""
                PREPARE bm25 AS (
                WITH scored_docs AS (
                    SELECT *, fts_main_documents.match_bm25(id, ?) AS score FROM documents) 
                SELECT id, score
                FROM scored_docs
                WHERE score IS NOT NULL
                ORDER BY score DESC
                LIMIT 1000)
            """)

    def run_topics(self):
        topics = get_topics(self.arguments['collection_name'])
        topics = sorted([[key, item['title']] for key, item in topics.items()])
        outfile = open(self.arguments['outfile'], 'w')
        for topic_id, topic_text in topics:
            self.cursor.execute(f"EXECUTE bm25('" + topic_text.replace("'", " ") + "')")
            results = self.cursor.fetchall()
            for rank, result in enumerate(results):
                outfile.write(f"{topic_id} Q0 {result[0]} {rank+1} {result[1]} oldduck\n")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        '--database',
                        required=True,
                        metavar='[file]',
                        help='Location of the database.')
    parser.add_argument('-c',
                        '--collection_name',
                        required=True,
                        metavar='[string]',
                        help='Name of the collection.')
    parser.add_argument('-o',
                        '--outfile',
                        required=True,
                        metavar='[file]',
                        help='Name of output file.')
    Search(**vars(parser.parse_args()))
