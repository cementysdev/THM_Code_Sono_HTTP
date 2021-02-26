import pandas as pd
import numpy as np

import database


class Query:

    def __init__(self, host, db_name, user, port, password):
        """Connexion à la base"""
        self.db = database.Database(host=host, database=db_name, user=user, port=port, password=password)

    def close(self):
        """Fin de la connexion à la base"""
        self.db.close()

    def get_var_traitement(self, id_treatment):
        
        """Accesseur variables de la table treatment"""
        sql = """
        SELECT
            project_id,
            processing -> 'windows_max',
            processing -> 'rolling_period',
            processing -> 'sonometer_name',
            processing -> 'sound_barriers',
            processing -> 'diurnal_threshold',
            processing -> 'evening_threshold',
            processing -> 'diurnal_period_end',
            processing -> 'evening_period_end',
            processing -> 'nocturnal_threshold',
            processing -> 'diurnal_period_start',
            processing -> 'evening_period_start',
            processing -> 'nocturnal_period_end',
            processing -> 'nocturnal_period_start'
        FROM treatments
        WHERE id = {}
        """.format(id_treatment)
        print('id_treatment')
        print(id_treatment)
        return self.db.select(sql)[0]

    def get_name_sono(self, id_sono):
        """Accesseur nom_sono"""
        sql = """SELECT name FROM sensors where id = {}""".format(id_sono)
        
        return self.db.select(sql)[0][0]

    def get_var_id(self, id_sono, variables_table_metric):

        ls_var_id = []

        for i_variables_output in variables_table_metric:
            sql = """
            SELECT 
            id,
            name
            FROM variables 
            WHERE sensor_id = {} and name = {}
            """.format(id_sono, "'"+i_variables_output+"'")
            ls_var_id.append(self.db.select(sql)[0])

        return ls_var_id


#select timestamp, value, variable_id from raw_data
    def read_raw_data(self, var_variables):
        """
        Cette fonction permet de lire les données station dans la table raw_data.
        """

        # Affichage
        print("\nLecture des données brutes")

        # Initialisation du dataframe
        df_raw = pd.DataFrame()
        print(tuple(var_variables[0][0]))

        # Requête sql
        sql = """
        SELECT
        *
        from raw_data
        WHERE variable_id in {}
        """.format(tuple(var_variables[0][0])) # avoiir un tuple des id SELECT * from raw_data WHERE variable_id in (1112,1131) ORDER BY variable_id DESC
        ls = self.db.select(sql)
        ls = ls[0:3600]

        # Remplissage du dataframe
        for val in ls:
            timestamp, value, variable_id = val
            LAeq = 'LAeq' 
            timestamp = timestamp.strftime("%Y-%m-%d %H:%M:%S")
            df_raw.loc[timestamp, LAeq] = value

        df_raw.sort_index(inplace=True)
        df_raw['timestamp'] = df_raw.index
        df_raw.reset_index(drop=True, inplace=True)  # remise à zéro de l'index
        nom_cols = list(df_raw.columns)
        df_raw = df_raw[nom_cols]  # modification de l'ordre des colonnes
        
        return df_raw
        

    def save_output(self, df_out):
        """
        Cette fonction permet d'enregistrer les variables LAeq et LAeq_glissant dans la table
        measures.
        Input : - df_out : dataframe contenant les résultats du calcul d'ajustement
                - id_treatment : identifiant de la table treatment
        Output : None
        """

        # Affichage
        print("\nInsertion des données dans la base")

        # Insertion
        print(df_out.iloc[0])
        self.db.copy_from_df(df_out, 'measures')
