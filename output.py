# -*- coding: utf-8 -*-
"""
Created on Thu Feb 11 15:44:16 2021

@author: Anis Amamra
"""
import pandas as pd

def create_output(df_raw_data, var_variables):


    # Initialisation du dataframe
    df_out = pd.DataFrame([], columns=["timestamp", "value", "variable_id"])
    timestamp = df_raw_data["timestamp"]
    LAeq = df_raw_data["LAeq"]
    print('---------------')
    print(LAeq[0])
    print('---------------')
    LAeqS = df_raw_data["LAeq_glissant"]
    print(LAeqS)

    df_out.loc[0, :] = [timestamp[0], LAeq[0], var_variables[0][0]]
    df_out.loc[1, :] = [timestamp[0], LAeqS[0], var_variables[1][0]]

    print(df_out)
            
    return df_out