import argparse

from db_manager import PaperDBManager
import settings


def _parse_user_args():
    """Parses the user arguments"""
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-vf', '--veto_id_file',
                            help='Contains the filename with the id to perform the db search (newline separated)',
                            required=True)
    arg_parser.add_argument('-mp', '--max_papers',
                            help='Max similar paper number per query',
                            default=50,
                            type=int)
    arg_parser.add_argument('-mr', '--max_results',
                            help='Max number of results',
                            default=20,
                            type=int)
    return arg_parser.parse_args()


def query_db():
    args = _parse_user_args()
    # Establish the db connection and create the data
    db_manager = PaperDBManager.create(database=settings.DB_NAME,
                                       password=settings.DB_PWD,
                                       username=settings.DB_USER,
                                       port=int(settings.DB_PORT),
                                       host=settings.DB_HOST)
    db_manager.out_keyword_search(id_file=args.veto_id_file,
                                  max_papers=args.max_papers,
                                  max_results=args.max_results)
    db_manager.close()


if __name__ == '__main__':
    query_db()
