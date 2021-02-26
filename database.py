import pandas as pd
import psycopg2
from psycopg2.extras import register_json
import io


class Database:

    def __init__(self, host, database, user, password, port):
        """Connexion à la base"""
        self.password = password
        self.host = host
        self.db_name = database
        self.user = user
        self.port = port
        self.conn = psycopg2.connect(host=self.host, dbname=self.db_name, user=self.user, password=self.password,
                                     port=self.port)
        print("Database Ready")

    def select(self, query, parameter=[]):
        """Sélection"""
        cursor = self.conn.cursor()
        try:
            register_json(oid=3802, array_oid=3807)
            cursor.execute(query, parameter)
            print(parameter)
        except:
            raise Exception("Can't execute query")
        rows = cursor.fetchall()
        return rows


    def insert(self, query):
        """Insertion"""
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
        except:
            raise Exception("Can't execute query")
        self.conn.commit()

    def copy_from_df(self, df, table):
        """Insertion d'un dataframe pandas"""
        cur = self.conn.cursor()
        try:
            s_buf = io.StringIO()
            df.to_csv(s_buf, header=False, index=False, na_rep="nan")
            s_buf.seek(0)
            cur.copy_from(s_buf, table, sep=",", columns=tuple(df.columns),)
        except:
            raise Exception("Can't insert dataframe")
        self.conn.commit()


    def close(self):
        """Déconnexion de la base"""
        self.conn.close()
        print("Connexion to database ended")
