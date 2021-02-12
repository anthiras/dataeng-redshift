"""Create database tables"""
from sql_queries import create_table_queries, drop_table_queries
from dbclient import DbClient


def drop_tables(db):
    """
    Drop database tables

    Arguments:
        db: Database client
    """
    for query in drop_table_queries:
        db.execute(query)


def create_tables(db):
    """
    Create database tables

    Arguments:
        db: Database client
    """
    for query in create_table_queries:
        db.execute(query)


def main():
    """Main program handler"""
    with DbClient() as db:
        drop_tables(db)
        create_tables(db)


if __name__ == "__main__":
    main()