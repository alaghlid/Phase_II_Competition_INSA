from os import name
import pandas as pd
import csv
import json
import datetime

#### ______________________________________________________________________________________________________________________
## Equipe : ALANOZY
## Membres :
#           - LAGHLID Ayoub.
#           - ZKIM Youssef.
#### ______________________________________________________________________________________________________________________

###########################################################################################################################
## Cette fonction sert à réidentifier les individus en générant un fichier Json qui contiendra les guesses.
## Cet attaque est conçu pour les équipes qui modifient rien dans le timestamp (date) et GPS (longitude - latittude) 
###########################################################################################################################

# Path de la base originale (à changer)
path_originale = "INSAnonym/c3465dad3864bb1e373891fdcfbfcca5f974db6a9e0b646584e07c5f554d7df7"

# Path de la base anonymisée (à changer)
path_anonymisee = "/media/alaghlid/Disque/Attack Dbs/Sibyl_483/files/S_user_40_2b9a2307d8c8d01b3885a20a7d22f25eccfa2d8c319c85521c3b1947994c4b82"

# Path du fichier JSON généré (à changer)
json_guesses = '../dbm.json'

def alanozy_reident():
    ## Lire le fichier CSV de la base anonymisée
    df_anonym = pd.read_csv(path_anonymisee, sep="\t", header=None).set_axis(['QId', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    
    ## Extraction des lignes non supprimées
    df_anonym = df_anonym[df_anonym['QId'] != 'DEL']

    df_anonym['Date'] = pd.to_datetime(df_anonym['Date'], errors='coerce', dayfirst=True)
    df_anonym['week'] = df_anonym['Date'].dt.strftime('%Y-%W')

    ## Lire le fichier CSV de la base originale
    df_originale = pd.read_csv(path_originale, sep="\t", header=None).set_axis(['Id', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    df_originale['Date'] = pd.to_datetime(df_originale['Date'], errors='coerce', dayfirst=True)
    df_originale['week'] = df_originale['Date'].dt.strftime('%Y-%W')

    ## Création des fichiers orig/ano pour chaque semaine
    i = 0
    for week in df_originale['week'].unique():
        df_originale[df_originale['week'] == week].to_csv(f'../fichiers_weeks_orig/week_{week}.csv', header=False, index=False, sep='\t')
        df_anonym[df_anonym['week'] == week].to_csv(f'../fichiers_weeks_ano/week_{week}.csv', header=False, index=False, sep='\t')
        i += 1

    ## récupération de la liste des semaines (weeks)
    weeks = df_originale['week'].unique()

    df_originale_week = df_originale.drop(columns=['week'])
    df_anonym_week = df_anonym.drop(columns=['week'])

    ## récupération des Ids
    ids = df_originale['Id'].unique()

    ## Création de guesses_reident qui contient le guesse de chaque individu existant dans chaque semaine
    guesses_reident = {}
    for id in ids:
        guesses_reident[str(id)] = {}
    for week in weeks:
        df_originale_week = pd.read_csv(f'../fichiers_weeks_orig/week_{week}.csv', sep='\t', names=[
                             "id", "date", "latittude", "longitude", "week"]).set_index('date')
        df_anonym_week = pd.read_csv(f'../fichiers_weeks_ano/week_{week}.csv', sep='\t', names=[
                            "pseudo_Id", "date", "latittude", "longitude", "week"]).set_index('date')
        merged_df = df_anonym_week.merge(df_originale_week, how='inner', on=[
                                'date', 'latittude', 'longitude']).set_index('id')
        merged_df = merged_df.drop(columns=['latittude', 'longitude'])
        m = merged_df.groupby(['id', 'pseudo_Id']).count().reset_index()
        m['pseudo_Id'] = m['pseudo_Id'].astype('str')
        for id in ids:
            guesses_reident[str(id)][str(week)] = (
                m[m['id'] == id])['pseudo_Id'].values.tolist()
        print(f"Réidentification terminée pour la semaine {week}")

    ## Génération du fichier JSON en écrivant le contenu de guesses_reident
    with open(json_guesses, 'w') as f:
        json.dump(guesses_reident, f)
    
alanozy_reident()
