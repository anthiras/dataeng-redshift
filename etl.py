"""ETL pipeline"""
from sql_queries import copy_table_queries, insert_table_queries
from dbclient import DbClient


def load_staging_tables(db):
    """
    Load data from S3 into Redshift staging tables

    Arguments:
        db: Database client
    """
    for query in copy_table_queries:
        db.execute(query)


def insert_tables(db):
    """
    Insert data into Redshift analytics tables from staging tables

    Arguments:
        db: Database client
    """
    for query in insert_table_queries:
        db.execute(query)


def main():
    """Main program handler"""
    with DbClient() as db:
        load_staging_tables(db)
        insert_tables(db)


if __name__ == "__main__":
    main()