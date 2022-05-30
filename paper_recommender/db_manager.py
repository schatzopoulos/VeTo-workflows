import csv
from collections import OrderedDict
import json
import pymongo
import string
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

_STOP_WORDS = set(stopwords.words('english'))
_PUNCTUATION = set(string.punctuation)


class PaperDBManager:
    """DB Wrapper for the paper database"""

    def __init__(self):
        self._db = None
        self._client = None
        self._paper_collection = None
        self._aminer_mapper_collection = None

    def __str__(self):
        return f'PaperDBManager(db_id={id(self._db)})'

    def close(self):
        self._client.close()

    def _insert_data_from_json(self, json_filenames):
        """Inserts data from the dblp json files"""

        for json_filename in json_filenames:
            print(f'Parsing file: {json_filename}')
            with open(json_filename, encoding='utf-8') as json_f:
                json_file = json_f.read()
                json_file = json_file.replace('\n', '')
                json_file = json_file.replace('}{', '},{')
                json_file = "[" + json_file + "]"
                data = json.loads(json_file)
                self._paper_collection.insert_many(data)

    def _insert_veto_aminer_id_mapping(self, aminer_ids_file):
        """Inserts the aminer to veto id mapper collection"""
        aminer_key = 'aminer_id'
        veto_key = 'id'
        doc_list = []
        with open(aminer_ids_file, newline='') as csvfile:
            reader = csv.DictReader(csvfile, delimiter='\t')
            for row in reader:
                doc = OrderedDict()
                doc[aminer_key] = row[aminer_key]
                doc[veto_key] = row[veto_key]
                doc_list.append(doc)
        self._aminer_mapper_collection.insert_many(doc_list)

    def _add_indexes(self):
        """Adds indexes to the paper collection"""
        self._paper_collection.create_index([('id', pymongo.ASCENDING)], unique=True, name='papers_id_uidx')
        self._paper_collection.create_index([('title', pymongo.TEXT), ('abstract', pymongo.TEXT)],
                                            default_language='english', name='papers_title_abstract_txt_idx')
        self._aminer_mapper_collection.create_index([('id', pymongo.ASCENDING)],
                                                    unique=True, name='aminer_mapper_id_uidx')
        self._aminer_mapper_collection.create_index([('aminer_id', pymongo.ASCENDING)],
                                                    unique=True, name='aminer_mapper_aminer_id_uidx')

    def perform_search_queries(self, titles_file):
        for title in open(titles_file):
            self._perform_search_query(title)
            break

    def _perform_search_query(self, search_term):
        pipeline = self._build_search_pipeline(search_term)
        res = self._paper_collection.aggregate(pipeline)
        print(res)

    @staticmethod
    def _remove_stopwords_and_punctuation(paper_title):
        """Removes stopwords and punctuation from a title"""
        word_tokens = word_tokenize(paper_title)
        final_tokens = []
        for w in word_tokens:
            if not w.lower() in _STOP_WORDS and not w.lower() in _PUNCTUATION:
                final_tokens.append(w)
        return ' '.join(final_tokens)

    @staticmethod
    def _build_search_pipeline(search_term):
        return [
            {'$match': {'$text': {'$search': f'{search_term}'}}},
            {'$project': {'id': 1, 'title': 1, '_id': 0, 'abstract': 1,
                          'rscore': {'$round': [{'$meta': 'textScore'}, 2]}, 'year': 1}},
            {'$sort': {'score': {'$meta': 'textScore'}}},
            {'$limit': 20}
        ]

    def build(self, json_filenames, aminer_ids_file):
        """Builds the database"""
        print('Adding JSON data ....')
        self._insert_data_from_json(json_filenames)
        print('Adding aminer to veto id mapping data ....')
        self._insert_veto_aminer_id_mapping(aminer_ids_file)
        print('Adding indexes ....')
        self._add_indexes()

    @classmethod
    def create(cls, database, password=None, username=None, host='localhost', port=27017):
        """
        Creates a PaperDBManager instance

        :param database: database name
        :param username: database user - defaults to None
        :param password: password for the specified database user - defaults to None
        :param host: host ip - defaults to localhost
        :param port: connection port - defaults to 27017
        :rtype: PaperDBManager
        """
        db_manager = cls()
        try:
            client = pymongo.MongoClient(host=host, port=port, username=username, password=password)
            db_manager._client = client
            db = client[database]
            db_manager._db = db
            db_manager._paper_collection = db['papers']
            db_manager._aminer_mapper_collection = db['aminer_mapper']
            return db_manager
        except Exception as error:
            print("Error connecting to MongoDB database", error)
