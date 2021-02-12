"""Database client"""
import configparser
import psycopg2

class DbClient:
    """
    Database client, that connects to Redshift when instantiated and disconnects after use.

    Database credentials are automatically loaded from the configuration file.
    """
    def __enter__(self):
        config = configparser.ConfigParser()
        config.read('dwh.cfg')

        self.conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
        self.conn.autocommit = True
        self.cur = self.conn.cursor()

        return self

    def __exit__(self, type, value, traceback):
        self.conn.close()

    def execute(self, query):
        """
        Execute a SQL query

        Arguments:
            query: The SQL query to execute
        """
        print(query)
        self.cur.execute(query)

    def execute_and_fetchall(self, query):
        """
        Execute a SQL query and fetch all result rows

        Arguments:
            query: The SQL query to execute
        """
        self.execute(query)
        return self.cur.fetchall()
