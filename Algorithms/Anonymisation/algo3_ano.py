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

    df = pd.read_csv(path_originale, sep='\t', header=None).set_axis(
        ['id', 'Date', 'longitude', 'lattitude'], axis=1, inplace=False)
    df['Date'] = pd.to_datetime(df['Date'])

    ## Créer une nouvelle colonne de Week
    df['Week'] = df['Date'].dt.strftime('%Y-%W')

    #############################################################################
    ## Modification des coordonnées de GPS
    #############################################################################

    gps = ['longitude', 'lattitude']
    df[gps] = df[gps].round(5) + 0.000077


    #############################################################################
    ## Préparation des lignes des IDs à supprimer - DEL -
    #############################################################################

    ## df_count : Pour compter le nombre des occurences de chaque individu dans chaque semaine

    df_count = (df.groupby(['id', 'Week']).size().reset_index(name='counts')).sort_values(['Week', 'counts'], ascending=(True, False))
    df_count = df_count.reset_index()
    list_to_del = []
    tmp = 0
    iterator_on_next = 0
    occ_map = dict()

    ## Boucle pour initialiser la liste des éléments à supprimer

    for i in df_count['id']:

        nb_occ_to_del = int(df_count['counts'][tmp]*(0.15))
        id_to_del = df_count['id'][tmp]
        val_inf = df_count['counts'][tmp]-nb_occ_to_del
        intervalle = range(val_inf, df_count['counts'][tmp]+1)
        if(tmp+1 <= len(df_count)):
            iterator_on_next = int(tmp+1)
        if(tmp < len(df_count)):
            if (df_count['counts'][tmp] >= val_inf):  # in range
                list_to_del.append(nb_occ_to_del)
                if(iterator_on_next == len(df_count)):
                    break
                while ((df_count['counts'][iterator_on_next] in intervalle) and (iterator_on_next != len(df_count))):
                    nb_occ_to_del = df_count['counts'][tmp] - \
                        df_count['counts'][iterator_on_next]
                    list_to_del.append(nb_occ_to_del)
                    iterator_on_next += 1
                if(iterator_on_next <= len(df_count)-1):
                    tmp = iterator_on_next
            else:
                tmp += 1
    w = 0
    df_count['to_del'] = list_to_del

    ## Création de la map des occurences
    occ_map = dict()
    for i in df_count['id']:
        chaine = str(df_count['id'][w])+"-"+str(df_count['Week'][w])
        occ_map[chaine] = df_count['to_del'][w]
        w += 1

    tmp2 = 0

    ## string.puncuation contient les caractères suivants : !"#$%&'()*+, -./:;<=>?@[\]^_`{|}~
    res = ''.join(secrets.choice(string.punctuation)
                  for x in range(taille_pseudoId))

    ## Cette Map pour s'assurer de l'unicité des pseudo Ids
    map_pseudoIds = {str(df['id'][tmp2])+str(df['Week'][tmp2]): res}

    for i in df['id']:

        ## Générations des pseudo-Ids et suppression des IDs

        if(occ_map[str(df['id'][tmp2])+"-"+str(df['Week'][tmp2])] > 0):
            ma_chaine = str(df['id'][tmp2])+"-"+str(df['Week'][tmp2])
            occ_map[ma_chaine] = int(occ_map[ma_chaine])-1
            df['id'][tmp2] = "DEL"
        else:
            #############################################################################
            ## Génération des Pseudo IDs pour les lignes non supprimées
            #############################################################################

            # key : c'est ID concaténé avec la semaine (Week)
            key = str(df['id'][tmp2])+str(df['Week'][tmp2])
            # Pseudo Id déjà générer pour cet individu (Existe dans map_pseudoIds)
            if(map_pseudoIds.get(key, -1) != -1):
                df['id'][tmp2] = map_pseudoIds[key]

            else:  # Pseudo Id généré pour la première fois pour cet individu dans cette semaine
                res = ''.join(secrets.choice(string.punctuation)
                              for x in range(taille_pseudoId))
                df['id'][tmp2] = res
                # Ajouter le nouveau pseudo Id à map_pseudoIds
                map_pseudoIds[key] = res


            #############################################################################
            ## Modification du timestamp (Date)
            #############################################################################

            ## Jours : [0, 6]
            day_position = df['Date'][tmp2].dayofweek

            ## Secondes : [00, 59]
            sec = df['Date'][tmp2].second

            ## Heures : [00,23]
            hour_tmp2 = df['Date'][tmp2].hour

            # Traitement de JJ (jours)
            if (day_position > 4):
                ## Inverser les jours de weekend (Samedi <-> Dimanche)
                if (day_position == 5):
                    df['Date'][tmp2] = df['Date'][tmp2] + \
                        datetime.timedelta(days=1)
                else:
                    df['Date'][tmp2] = df['Date'][tmp2] - \
                        datetime.timedelta(days=1)
            else:
                ## Changement des jours de la semaine est paramétré par la valeur des secondes
                if (sec in range(0, 20)):
                    if (day_position < 3):
                        df['Date'][tmp2] = df['Date'][tmp2] + \
                            datetime.timedelta(days=2)
                    else:
                        df['Date'][tmp2] = df['Date'][tmp2] - \
                            datetime.timedelta(days=1)
                elif (sec in range(20, 40)):
                    if (day_position < 4):
                        df['Date'][tmp2] = df['Date'][tmp2] + \
                            datetime.timedelta(days=1)
                    else:
                        df['Date'][tmp2] = df['Date'][tmp2] - \
                            datetime.timedelta(days=2)
                else:
                    if (day_position == 0):
                        df['Date'][tmp2] = df['Date'][tmp2] + \
                            datetime.timedelta(days=2)
                    elif (day_position == 1):
                        df['Date'][tmp2] = df['Date'][tmp2] + \
                            datetime.timedelta(days=1)
                    elif (day_position == 3):
                        df['Date'][tmp2] = df['Date'][tmp2] - \
                            datetime.timedelta(days=1)
                    elif (day_position == 4):
                        df['Date'][tmp2] = df['Date'][tmp2] - \
                            datetime.timedelta(days=2)

            # Traitement de HH:MM:SS (heure, minutes, secondes)
            ## Reste des tranches horaires
            if (hour_tmp2 in range(6, 9)):
                df['Date'][tmp2] = df['Date'][tmp2].replace(
                    hour=7, minute=59, second=59)
            elif (hour_tmp2 in range(18, 22)):
                df['Date'][tmp2] = df['Date'][tmp2].replace(
                    hour=20, minute=59, second=59)
            elif (hour_tmp2 in range(17, 18)):
                df['Date'][tmp2] = df['Date'][tmp2].replace(
                    hour=16, minute=59, second=59)

            ## Période de travail (Work). Remarque : on applique ce traitement aussi sur la période de Weekend !
            if (hour_tmp2 in range(9, 12)):
                df['Date'][tmp2] = df['Date'][tmp2].replace(
                    hour=11, minute=59, second=59)
            elif(hour_tmp2 in range(12, 17)):
                df['Date'][tmp2] = df['Date'][tmp2].replace(
                    hour=14, minute=59, second=59)

            ## Période de Nuit
            elif(hour_tmp2 in range(22, 00)):
                df['Date'][tmp2] = df['Date'][tmp2].replace(
                    hour=23, minute=59, second=59)
            elif(hour_tmp2 in range(00, 3)):
                df['Date'][tmp2] = df['Date'][tmp2].replace(
                    hour=2, minute=59, second=59)
            elif(hour_tmp2 in range(3, 6)):
                df['Date'][tmp2] = df['Date'][tmp2].replace(
                    hour=4, minute=59, second=59)
        tmp2 += 1

    ## Suppression de la colonne de Week
    df = df.drop(columns=['Week'])

    ## Générer le fichier CSV de la base anonymisée
    df.to_csv(path_anonymisee, sep='\t', header=False, index=False)


alanozy_ano()
