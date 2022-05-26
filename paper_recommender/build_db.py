import argparse

from db_manager import PaperDBManager
import settings


def _parse_user_args():
    """Parses the user arguments"""
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-f', '--files', help='the json files to parse the data from (space separated)',
                            required=True)
    return arg_parser.parse_args()


def build_db():
    args = _parse_user_args()
    # Establish the db connection and create the data
    db_manager = PaperDBManager.create(database=settings.DB_NAME,
                                       password=settings.DB_PWD,
                                       username=settings.DB_USER,
                                       port=int(settings.DB_PORT),
                                       host=settings.DB_HOST)
    db_manager.insert_data_from_json(args.files.split(' '))
    db_manager.add_indexes()
    db_manager.close()


if __name__ == '__main__':
    build_db()
