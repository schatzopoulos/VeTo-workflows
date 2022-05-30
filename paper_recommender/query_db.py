import argparse

from db_manager import PaperDBManager
import settings


def _parse_user_args():
    """Parses the user arguments"""
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-vf', '--veto_id_file',
                            help='Contains the filename with the id to perform the db search (newline separated)',
                            required=True)
    return arg_parser.parse_args()


def query_db():
    args = _parse_user_args()
    # Establish the db connection and create the data
    db_manager = PaperDBManager.create(database=settings.DB_NAME,
                                       password=settings.DB_PWD,
                                       username=settings.DB_USER,
                                       port=int(settings.DB_PORT),
                                       host=settings.DB_HOST)
    db_manager.perform_search_queries(args.veto_id_file)
    db_manager.close()


if __name__ == '__main__':
    query_db()
