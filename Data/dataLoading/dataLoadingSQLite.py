import sqlite3

import pandas as pd

import sys


def main():
    conn = create_connection('BBSQLitedb.db')
    cur = conn.cursor()

    perGame = pd.read_csv("../webscrapers/csvs/player_per_game.csv",
                          names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                 'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                                 '2P', '2PA', '2P%', 'EFG%', 'FT', 'FTA', 'FT%',
                                 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'], header=None)
    perGame = perGame.drop(columns='league')

    playerIDDict = {}

    id = 0
    for n in perGame['name']:
        if not (n in playerIDDict.keys()):
            playerIDDict[n] = id
            id += 1

    insertPlayersStr = "INSERT INTO `Players` (\'pid\', \'name\') VALUES\n"

    for key, value in playerIDDict.items():
        insertPlayersStr = insertPlayersStr + "(" + str(value) + ", \"" + key + "\"),\n"

    insertPlayersStr = insertPlayersStr[:-2] + ";"
    #cur.execute(insertPlayersStr)
    conn.commit()

    insertPerGameStr = "INSERT INTO `PerGame` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                       "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', \'EFG%\', " \
                       "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                       "\'PF\', \'PTS\') VALUES\n"

    for index, row in perGame.iterrows():
        insertPerGameStr = insertPerGameStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 30):
            if i == 3 or i == 4:
                insertPerGameStr = insertPerGameStr + "\'" + str(values[i]) + "\', "
            else:
                insertPerGameStr = insertPerGameStr + str(values[i]) + ", "
        insertPerGameStr = insertPerGameStr[:-2] + "),\n"

    insertPerGameStr = insertPerGameStr[:-2] + ";"
    insertPerGameStr = insertPerGameStr.replace("nan", "NULL")
    #cur.execute(insertPerGameStr)
    conn.commit()

    Totals = pd.read_csv("../webscrapers/csvs/player_total.csv",
                          names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                 'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                                 '2P', '2PA', '2P%', 'EFG%', 'FT', 'FTA', 'FT%',
                                 'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'], header=None)
    Totals = Totals.drop(columns='league')

    insertTotalsStr = "INSERT INTO `Totals` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                       "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', \'EFG%\', " \
                       "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                       "\'PF\', \'PTS\') VALUES\n"

    for index, row in Totals.iterrows():
        insertTotalsStr = insertTotalsStr + "(" + str(playerIDDict[row["name"]]) + ", "
        values = row.values
        for i in range(1, 30):
            if i == 3 or i == 4:
                insertTotalsStr = insertTotalsStr + "\'" + str(values[i]) + "\', "
            else:
                insertTotalsStr = insertTotalsStr + str(values[i]) + ", "
        insertTotalsStr = insertTotalsStr[:-2] + "),\n"

        insertTotalsStr = insertTotalsStr[:-2] + ";"
        insertTotalsStr = insertTotalsStr.replace("nan", "NULL")
        #cur.execute(insertTotalsStr)
        conn.commit()

        per36 = pd.read_csv("../webscrapers/csvs/per_36.csv",
                             names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                    'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                                    '2P', '2PA', '2P%', 'FT', 'FTA', 'FT%',
                                    'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS'], header=None)
        per36 = per36.drop(columns='league')

        insertPer36Str = "INSERT INTO `Per36` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                          "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', " \
                          "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                          "\'PF\', \'PTS\') VALUES\n"

        for index, row in per36.iterrows():
            insertPer36Str = insertPer36Str + "(" + str(playerIDDict[row["name"]]) + ", "
            values = row.values
            for i in range(1, 29):
                if i == 1:
                    insertPer36Str = insertPer36Str + str(values[i][-2:]) + ", "
                elif i == 3 or i == 4:
                    insertPer36Str = insertPer36Str + "\'" + str(values[i]) + "\', "
                else:
                    insertPer36Str = insertPer36Str + str(values[i]) + ", "
            insertPer36Str = insertPer36Str[:-2] + "),\n"

        insertPer36Str = insertPer36Str[:-2] + ";"
        insertPer36Str = insertPer36Str.replace("nan", "NULL")
        #cur.execute(insertPer36Str)
        conn.commit()

        per100Poss = pd.read_csv("../webscrapers/csvs/per_100_poss.csv",
                            names=['name', 'year', 'age', 'team', 'league', 'position', 'GP',
                                   'GS', 'MP', 'FG', 'FGA', 'FG%', '3P', '3PA', '3P%',
                                   '2P', '2PA', '2P%', 'FT', 'FTA', 'FT%',
                                   'ORB', 'DRB', 'TRB', 'AST', 'STL', 'BLK', 'TOV', 'PF', 'PTS', 'ORTG', 'DRTG'], index_col=False, header=None)
        per100Poss = per100Poss.drop(columns='league')

        insertPer100PossStr = "INSERT INTO `Per100Poss` (\'pid\', \'year\', \'age\', \'team\', \'position\', \'GP\', \'GS\', \'MP\', " \
                         "\'FG\', \'FGA\', \'FG%\', \'3P\', \'3PA\', \'3P%\', \'2P\', \'2PA\', \'2P%\', " \
                         "\'FT\', \'FTA\', \'FT%\', \'ORB\', \'DRB\', \'TRB\', \'AST\', \'STL\', \'BLK\', \'TOV\', " \
                         "\'PF\', \'PTS\', \'ORTG\', \'DRTG\') VALUES\n"

        for index, row in per100Poss.iterrows():
            insertPer100PossStr = insertPer100PossStr + "(" + str(playerIDDict[row["name"]]) + ", "
            values = row.values
            for i in range(1, 31):
                if i == 3 or i == 4:
                    insertPer100PossStr = insertPer100PossStr + "\'" + str(values[i]) + "\', "
                else:
                    insertPer100PossStr = insertPer100PossStr + str(values[i]) + ", "
            insertPer100PossStr = insertPer100PossStr[:-2] + "),\n"

        insertPer100PossStr = insertPer100PossStr[:-2] + ";"
        insertPer100PossStr = insertPer100PossStr.replace("nan", "NULL")
        #cur.execute(insertPer100PossStr)
        conn.commit()



def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by the db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except:
        print(sys.exc_info()[0])

    return conn


main()
