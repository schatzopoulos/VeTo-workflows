import argparse

from db_manager import PaperDBManager
import settings


def _parse_user_args():
    """Parses the user arguments"""
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-f', '--files', help='the json files to parse the data from (space separated)',
                            required=True),
    arg_parser.add_argument('-af', '--aminer_ids_file', help='the csv file containing the aminer to veto id mapping',
                            required=True)
    arg_parser.add_argument('-tw', '--title_weight', nargs='?', type=int, default=1,
                            help='score weight for the keyword relevance')
    arg_parser.add_argument('-aw', '--abstract_weight', nargs='?', type=int, default=1,
                            help='score weight for the keyword relevance')
    return arg_parser.parse_args()


def build_db():
    args = _parse_user_args()
    # Establish the db connection and create the data
    db_manager = PaperDBManager.create(
        database=settings.DB_NAME,
        password=settings.DB_PWD,
        username=settings.DB_USER,
        port=int(settings.DB_PORT),
        host=settings.DB_HOST,
        title_weight=args.title_weight,
        abstract_weight=args.abstract_weight
    )
    db_manager.build(args.files.split(' '), args.aminer_ids_file)
    db_manager.close()


if __name__ == '__main__':
    build_db()
