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
    PAPER_KEY = 'papers'
    AMINER_MAPPER_KEY = 'aminer_mapper'

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
                data = []
                for item in json_f:
                    data.append(
                        OrderedDict(json.loads(item))
                    )
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

    def perform_search_queries(self, id_file, max_papers=20):
        """Performs search queries to the db"""
        # read the veto ids and related them to aminer ids
        veto_ids = [veto_id.strip() for veto_id in open(id_file)]
        mapping_pipeline = self._build_mapping_pipeline(veto_ids)

        excluded_aminer_ids = []
        search_titles = []
        for item in self._aminer_mapper_collection.aggregate(mapping_pipeline):
            excluded_aminer_ids.append(item['aminer_id'])
            search_titles.append(self._remove_stopwords_and_punctuation(item['title']))

        res = {}
        for title in search_titles:
            pipeline = self._build_search_pipeline(title, excluded_aminer_ids, max_papers)
            max_score = max_papers
            for item in self._paper_collection.aggregate(pipeline):
                paper = item['id']
                if paper in res.keys():
                    res[paper] += max_score
                else:
                    res[paper] = max_score
                max_score -= 1
                if max_score == 0:
                    break
        print(json.dumps(res, indent=4))

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
    def _build_search_pipeline(search_term, excluded_ids, max_papers):
        """Search pipeline"""
        return [
            {'$match': {'id': {'$nin': excluded_ids}, '$text': {'$search': search_term}}},
            {'$project': {'id': 1, '_id': 0}},
            {'$sort': {'score': {'$meta': 'textScore'}}},
            {'$limit': max_papers},
            {'$lookup': {
                'from': 'aminer_mapper',
                'localField': 'id',
                'foreignField': 'aminer_id',
                'as': 'mapping'
            }
            },
            {'$unwind': '$mapping'},
            {'$replaceRoot': {'newRoot': {'id': '$mapping.id'}}},
            {'$project': {'_id': 0, 'id': 1}}
        ]

    def _build_mapping_pipeline(self, veto_ids):
        """Mapping pipeline"""
        return [
            {'$match': {'id': {'$in': veto_ids}}},
            {'$lookup': {
                'from': self.PAPER_KEY,
                'localField': 'aminer_id',
                'foreignField': 'id',
                'as': 'mapping'
            }
            },
            {'$unwind': '$mapping'},
            {'$replaceRoot': {'newRoot': {'title': '$mapping.title', 'aminer_id': '$mapping.id'}}},
            {'$project': {'_id': 0, 'title': 1, 'aminer_id': 1}},
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
            db_manager._paper_collection = db[cls.PAPER_KEY]
            db_manager._aminer_mapper_collection = db[cls.AMINER_MAPPER_KEY]
            return db_manager
        except Exception as error:
            print("Error connecting to MongoDB database", error)
