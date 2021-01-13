import argparse
import duckdb
import gzip
import os
import CommonIndexFileFormat_pb2 as Ciff


class Index:

    _COLUMN_TYPES = [
        ['STRING', 'INT', 'INT'],
        ['INT', 'INT', 'STRING'],
        ['INT', 'INT', 'INT']
    ]

    def __init__(self, **kwargs):
        self.arguments = self.get_arguments(kwargs)
        if os.path.isfile(self.arguments['database']):
            raise IOError('Database already exists.')
        self.connection = duckdb.connect(self.arguments['database'])
        self.cursor = self.connection.cursor()
        self.create_tables()
        self.fill_tables()
        self.create_stats_table()

    @staticmethod
    def get_arguments(kwargs):
        arguments = {
            'database': None,
            'protobuf_file': None,
        }
        for key, item in arguments.items():
            if kwargs.get(key) is not None:
                arguments[key] = kwargs.get(key)
        if arguments['database'] is None:
            raise IOError('database path needs to be provided')
        if arguments['protobuf_file'] is None:
            raise IOError('protobuf file needs to be provided')
        return arguments

    @staticmethod
    def decode(buffer, pos):
        mask = (1 << 32) - 1
        result = 0
        shift = 0
        while True:
            b = buffer[pos]
            result |= ((b & 0x7f) << shift)
            pos += 1
            if not (b & 0x80):
                result &= mask
                result = int(result)
                return result, pos
            shift += 7
            if shift >= 64:
                raise IOError('Too many bytes when decoding.')

    def create_table(self, table_name, column_names, column_types):
        try:
            self.cursor.execute(f'SELECT * FROM {table_name} LIMIT 1;')
            self.connection.rollback()
            raise IOError('Table already exists.')
        except:
            pass
        query = f'CREATE TABLE {table_name} ({", ".join([f"{a} {b}" for a, b in zip(column_names, column_types)])});'
        self.cursor.execute(query)

    def create_stats_table(self):
        query = 'CREATE TABLE stats AS (SELECT COUNT(docs.doc_id) AS num_docs, SUM(docs.len) / COUNT(docs.len) AS avgdl FROM docs);'
        self.cursor.execute(query)

    def create_tables(self):
        table_names = ['docs', 'term_dict', 'term_doc']
        column_names = [['collection_id', 'doc_id', 'len'],
                        ['term_id', 'df', 'string'],
                        ['term_id', 'doc_id', 'tf']]
        self.connection.begin()
        for table_name, c_names, c_types in zip(table_names, column_names, self._COLUMN_TYPES):
            self.create_table(table_name, c_names, c_types)
        self.connection.commit()

    def fill_tables(self):
        if self.arguments['protobuf_file'].endswith('.gz'):
            with gzip.open(self.arguments['protobuf_file'], 'rb') as f:
                data = f.read()
        else:
            with open(self.arguments['protobuf_file'], 'rb') as f:
                data = f.read()

        # start with reading header info
        next_pos, pos = 0, 0
        header = Ciff.Header()
        next_pos, pos = self.decode(data, pos)
        header.ParseFromString(data[pos:pos+next_pos])
        pos += next_pos

        # read posting lists
        postings_list = Ciff.PostingsList()
        for term_id in range(header.num_postings_lists):
            self.connection.begin()
            next_pos, pos = self.decode(data, pos)
            postings_list.ParseFromString(data[pos:pos+next_pos])
            pos += next_pos
            q = f'INSERT INTO term_dict ' \
                f'(term_id,df,string) ' \
                f"VALUES ({term_id},{postings_list.df},'{postings_list.term}')"
            try:
                self.cursor.execute(q)
            except:
                print(q)
            for posting in postings_list.postings:
                q = f'INSERT INTO term_doc ' \
                    f'(term_id,doc_id,tf) ' \
                    f'VALUES ({term_id},{posting.docid},{posting.tf})'
                self.cursor.execute(q)
            self.connection.commit()

        # read doc information
        doc_record = Ciff.DocRecord()
        self.connection.begin()
        for n in range(header.num_docs):
            if n % 1000 == 0:
                self.connection.commit()
                self.connection.begin()
            next_pos, pos = self.decode(data, pos)
            doc_record.ParseFromString(data[pos:pos+next_pos])
            pos += next_pos
            q = f'INSERT INTO docs ' \
                f'(collection_id,doc_id,len) ' \
                f"VALUES ('{doc_record.collection_docid}',{doc_record.docid},{doc_record.doclength})"
            self.cursor.execute(q)
        self.connection.commit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-d',
                        '--database',
                        required=True,
                        metavar='[file]',
                        help='Location of the database.')
    parser.add_argument('-p',
                        '--protobuf_file',
                        required=True,
                        metavar='[file]',
                        help='Filename for the csv file containing the data for the docs table.')
    Index(**vars(parser.parse_args()))
