import datetime
import csv
import pandas as pd
import numpy as np
import string
import secrets
import random

#### ______________________________________________________________________________________________________________________
## Equipe : ALANOZY
## Membres : 
#           - LAGHLID Ayoub.               
#           - ZKIM Youssef.
#### ______________________________________________________________________________________________________________________

###########################################################################################################################
## Cette fonction sert à anonymiser les données de la base originale afin de générer une nouvelle base anonymisée.
## Processus de l'anonymisation : 
## - Générer un pseudo Id unique par semaine pour chaque individu.
## - Modifier le timestamp.
## - Modifier légèrement les coordonnées de GPS.
## - Supprimer quelques lignes en remplaçant leurs ID par DEL
###########################################################################################################################

# Pour désactiver Warning
pd.options.mode.chained_assignment = None  # default='warn'

# Taille du pseudo Id
taille_pseudoId = 15

# Path de la base originale (à changer)
path_originale = 'INSAnonym/c3465dad3864bb1e373891fdcfbfcca5f974db6a9e0b646584e07c5f554d7df7'

# Path de la base anonymisée (à changer)
path_anonymisee = 'Phase2/S2ubmission_2_PI.csv'

def alanozy_ano():
    ## Lire le fichier CSV de la base originale 

    df = pd.read_csv(path_originale, sep='\t', header=None).set_axis(['id', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    df['Date'] = pd.to_datetime(df['Date'])

    ## Créer une nouvelle colonne de Week 
    df['Week'] = df['Date'].dt.strftime('%Y-%W')

    #############################################################################
    ## Supprimer des lignes des IDs  - DEL -
    #############################################################################

    ## Suppression des Ids satisfaisant la régle suivante : index % 6 == 0  => Pourcentage de suppression : 20% ( 2 lignes dans chaque bloc de 10 Lignes )
    df.loc[df.index % 6 == 0, 'id'] = "DEL"

    #############################################################################
    ## Modification des coordonnées de GPS
    #############################################################################

    gps = ['longitude', 'lattitude']
    df[gps] = df[gps].round(4) + random.uniform(0.0000, 0.0004)

    tmp = 0

    ## string.puncuation contient les caractères suivants : !"#$%&'()*+, -./:;<=>?@[\]^_`{|}~
    res = ''.join(secrets.choice(string.punctuation)for x in range(taille_pseudoId))

    ## Cette Map pour s'assurer de l'unicité des pseudo Ids 
    map_pseudoIds = {str(df['id'][tmp])+str(df['Week'][tmp]): res}

    for i in df['id']:
        if (df['id'][tmp] != "DEL"):
            #############################################################################
            ## Génération des Pseudo IDs pour les lignes non supprimées
            #############################################################################

            # key : c'est ID concaténé avec la semaine (Week)
            key = str(df['id'][tmp])+str(df['Week'][tmp])
            if(map_pseudoIds.get(key, -1) != -1): # Pseudo Id déjà générer pour cet individu (Existe dans map_pseudoIds)
                df['id'][tmp] = map_pseudoIds[key]
            
            else:  # Pseudo Id généré pour la première fois pour cet individu dans cette semaine
                res = ''.join(secrets.choice(string.punctuation)for x in range(taille_pseudoId))
                df['id'][tmp] = res
                # Ajouter le nouveau pseudo Id à map_pseudoIds
                map_pseudoIds[key] = res
            
            #############################################################################
            ## Modification du timestamp (Date)
            #############################################################################

            ## Jours : [0, 6]
            day_position = df['Date'][tmp].dayofweek

            ## Secondes : [00, 59]
            sec = df['Date'][tmp].second

            ## Heures : [00,23]
            hour_tmp = df['Date'][tmp].hour

            # Traitement de JJ (jours)
            if (day_position > 4):
                ## Inverser les jours de weekend (Samedi <-> Dimanche)
                if (day_position == 5):
                    df['Date'][tmp] = df['Date'][tmp] + datetime.timedelta(days=1)
                else:
                    df['Date'][tmp] = df['Date'][tmp] - datetime.timedelta(days=1)
            else:
                ## Changement des jours de la semaine est paramétré par la valeur des secondes 
                if (sec in range(0, 20)):
                    if (day_position < 3):
                        df['Date'][tmp] = df['Date'][tmp] + datetime.timedelta(days=2)
                    else:
                        df['Date'][tmp] = df['Date'][tmp] - datetime.timedelta(days=1)
                elif (sec in range(20, 40)):
                    if (day_position < 4):
                        df['Date'][tmp] = df['Date'][tmp] + datetime.timedelta(days=1)
                    else:
                        df['Date'][tmp] = df['Date'][tmp] - datetime.timedelta(days=2)
                else:
                    if (day_position == 0):
                        df['Date'][tmp] = df['Date'][tmp] + datetime.timedelta(days=2)
                    elif (day_position == 1):
                        df['Date'][tmp] = df['Date'][tmp] + datetime.timedelta(days=1)
                    elif (day_position == 3):
                        df['Date'][tmp] = df['Date'][tmp] - datetime.timedelta(days=1)
                    elif (day_position == 4):
                        df['Date'][tmp] = df['Date'][tmp] - datetime.timedelta(days=2)

            # Traitement de HH:MM:SS (heure, minutes, secondes)
            ## Reste des tranches horaires
            if (hour_tmp in range(6, 9)):
                df['Date'][tmp] = df['Date'][tmp].replace(
                    hour=7, minute=59, second=59)
            elif (hour_tmp in range(18, 22)):
                df['Date'][tmp] = df['Date'][tmp].replace(
                    hour=20, minute=59, second=59)
            elif (hour_tmp in range(17, 18)):
                df['Date'][tmp] = df['Date'][tmp].replace(
                    hour=16, minute=59, second=59)

            ## Période de travail (Work). Remarque : on applique ce traitement aussi sur la période de Weekend !
            if (hour_tmp in range(9, 12)):
                df['Date'][tmp] = df['Date'][tmp].replace(
                    hour=11, minute=59, second=59)
            elif(hour_tmp in range(12, 17)):
                df['Date'][tmp] = df['Date'][tmp].replace(
                    hour=14, minute=59, second=59)

            ## Période de Nuit
            elif(hour_tmp in range(22, 00)):
                df['Date'][tmp] = df['Date'][tmp].replace(
                    hour=23, minute=59, second=59)
            elif(hour_tmp in range(00, 3)):
                df['Date'][tmp] = df['Date'][tmp].replace(
                    hour=2, minute=59, second=59)
            elif(hour_tmp in range(3, 6)):
                df['Date'][tmp] = df['Date'][tmp].replace(
                    hour=4, minute=59, second=59)
        tmp = tmp+1

    ## Suppression de la colonne de Week 
    df = df.drop(columns=['Week'])

    ## Générer le fichier CSV de la base anonymisée
    df.to_csv(path_anonymisee, sep='\t', header=False, index=False)

alanozy_ano()
