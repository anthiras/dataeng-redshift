"""Example analysis"""
from sql_queries import analysis_queries
from dbclient import DbClient

def execute_queries(db):
    """
    Execute analysis queries

    Arguments:
        db: Database client
    """
    for query in analysis_queries:
        results = db.execute_and_fetchall(query)
        for row in results:
            print(row)


def main():
    """Main program handler"""
    with DbClient() as db:
        execute_queries(db)


if __name__ == "__main__":
    main()