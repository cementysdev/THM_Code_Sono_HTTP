# -*- coding: utf-8 -*-



import requests
import time
import datetime
from sql_queries import *
import output
import xmltodict
import json
from datetime import datetime


######################################### Paramètres de la lambda ######################################################
# Paramètre de la lambda
id_treatment = 1
localhost = "localhost"
database_name = "thm_database"
user = "postgres"
port = "5432"
password = "Cementys-dev-password"
########################################################################################################################


# Message de début
print("******************* CODE Sono *******************")

start_time = time.time()  # lancement du chronomètre

now = datetime.now()  # date du traitement

############################# Récupérer les parametres de la base treatemnts depuis le formulaire ##############
########### Connection à la base
query = Query(localhost, database_name, user, port, password)

########### Attributs de la table treatments
var_traitement = query.get_var_traitement(id_treatment)
id_project = int(var_traitement[0])
windows_max = int(var_traitement[1])
rolling_period = int(var_traitement[2])
id_sono = str(var_traitement[3])
sound_barriers = str(var_traitement[4])
diurnal_threshold = float(var_traitement[5])
evening_threshold = float(var_traitement[6])
diurnal_period_end = str(var_traitement[7])
evening_period_end = str(var_traitement[8])
nocturnal_threshold = float(var_traitement[9])
diurnal_period_start = str(var_traitement[10])
evening_period_start = str(var_traitement[11])
nocturnal_period_end = str(var_traitement[12])
nocturnal_period_start = str(var_traitement[13])
#url_sono = str(var_traitement[14])
URL_sono = "http://teltsonopf1.dyndns.org"


##### recuperer le nom du sono #####
# Depuis la table sensor récupérer le nom du sono a partir de son id insérer depuis le formulaire
name_sono = query.get_name_sono(id_sono)
print('name_sono :', name_sono)

#Depuis la table variables récupérer les id des variables de sortie (Leq et LAeq glissant)
# l'ordre des variables_table_metric doit etre respecter
variables_table_metric = ['LAeq', 'LAeq_glissant']
var_variables = query.get_var_id(id_sono, variables_table_metric)
print('id LAeq et LAeq glissant')
print(var_variables)

###################################################  Chargement des RawData depuis une requte http #######################
# <PublicRealTimeValues version="1">
# <LocalTime>2021/02/17 11:40:27.0</LocalTime>
# <Values>41.8;42.2</Values>
# </PublicRealTimeValues>
###############################################################
############# Lancer la requete #########################
get_values = '/pub/GetRealTimeValues.asp'
get_data = requests.get(URL_sono + get_values)
######### Extraction des donnees #############
xpars = xmltodict.parse(get_data.text)
json = json.dumps(xpars)

LocalTime = xpars['PublicRealTimeValues']['LocalTime']
LocalTime = LocalTime[0:-2].replace("/", "-")
print("LocalTime")
print(LocalTime)
TIMESTAMP = datetime.strptime(LocalTime, '%Y-%m-%d %H:%M:%S')

Niveaux_sonores = xpars['PublicRealTimeValues']['Values']
LAeq = Niveaux_sonores.split(';')[1]
LAeq_glissant = Niveaux_sonores.split(';')[0]

dict_raw_dat = {'timestamp': TIMESTAMP,
     'LAeq_glissant': LAeq_glissant,
     'LAeq':LAeq
     }

df_data_acoustique = pd.DataFrame(dict_raw_dat, index=[TIMESTAMP], columns=["timestamp","LAeq_glissant", "LAeq"])


################# Création du dataframe pour la table mesure##########
df_output = output.create_output(df_data_acoustique, var_variables)
#print(df_output)
print('df_output')
#print(df_output)

################ Enregistrement dans la base de données #############
query.save_output(df_output)

#######################
### MESSAGES DE FIN ###
#######################

# Fin de la connexion à la base
query.close()