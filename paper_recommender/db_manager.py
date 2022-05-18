import json
from pymongo import MongoClient


class PaperDBManager:
    """DB Wrapper for the paper database"""

    def __init__(self):
        self._db = None
        self._client = None

    def __str__(self):
        return f"PaperDBManager(db_id={id(self._db)})"

    def close(self):
        self._client.close()

    def insert_data_from_json(self, json_filenames):
        """Inserts data from the dblp json files"""
        paper_collection = self._db['papers']
        for json_filename in json_filenames:
            print(f'Parsing file: {json_filename}')
            with open(json_filename, encoding='utf-8') as json_f:
                json_file = json_f.read()
                json_file = json_file.replace('\n', '')
                json_file = json_file.replace('}{', '},{')
                json_file = "[" + json_file + "]"
                data = json.loads(json_file)
                paper_collection.insert_many(data)

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
            client = MongoClient(host=host, port=port, username=username, password=password)
            db_manager._client = client
            db_manager._db = client[database]
            return db_manager
        except Exception as error:
            print("Error connecting to MongoDB database", error)
