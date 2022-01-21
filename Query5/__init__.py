import logging
from py2neo import Graph
from py2neo.bulk import create_nodes, create_relationships
from py2neo.data import Node
import os
import pyodbc as pyodbc
import azure.functions as func


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    gender=req.params.get('value')
    actor=req.params.get('actor')
    director=req.params.get('director')
    if not gender and not actor and not director:
        error = "Please enter at least 1 argument."
        return func.HttpResponse(error, status_code=400)

    server = os.environ["TPBDD_SERVER"]
    database = os.environ["TPBDD_DB"]
    username = os.environ["TPBDD_USERNAME"]
    password = os.environ["TPBDD_PASSWORD"]
    driver= '{ODBC Driver 17 for SQL Server}'

    if len(server)==0 or len(database)==0 or len(username)==0 or len(password)==0:
        return func.HttpResponse("Au moins une des variables d'environnement n'a pas été initialisée.", status_code=500)
        
    errorMessage = ""
    dataString = ""
    try:
        logging.info("Test de connexion avec pyodbc...")
        with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
            cursor = conn.cursor()
            if gender:
                if actor:
                    if director:
                        myRequest = "SELECT AVG(runtimeMinutes) FROM dbo.tTitles, dbo.tGenres WHERE dbo.tTitles.tconst = dbo.tGenres.tconst AND genre = " + str(gender) + " AND dbo.tTitles.tconst IN (SELECT tconst FROM dbo.tPrincipals WHERE nconst = " + str(actor) + " AND category = 'acted in' INTERSECT SELECT tconst FROM dbo.tPrincipals WHERE nconst = " + str(director) " AND category = 'directed')"
                    else:
                        myRequest = "SELECT AVG(runtimeMinutes) FROM dbo.tTitles, dbo.tGenres WHERE dbo.tTitles.tconst = dbo.tGenres.tconst AND genre = " + str(gender) + " AND dbo.tTitles.tconst IN (SELECT tconst FROM dbo.tPrincipals WHERE nconst = " + str(actor) + " AND category = 'acted in')"
                else:
                    if director:
                        myRequest = "SELECT AVG(runtimeMinutes) FROM dbo.tTitles, dbo.tGenres WHERE dbo.tTitles.tconst = dbo.tGenres.tconst AND genre = " + str(gender) + " AND dbo.tTitles.tconst IN (SELECT tconst FROM dbo.tPrincipals WHERE nconst = " + str(director) " AND category = 'directed')"
                    else:
                        myRequest = "SELECT AVG(runtimeMinutes) FROM dbo.tTitles, dbo.tGenres WHERE dbo.tTitles.tconst = dbo.tGenres.tconst AND genre = " + str(gender)
            else:
                if actor:
                    if director:
                        myRequest = "SELECT AVG(runtimeMinutes) FROM dbo.tTitles, dbo.tGenres WHERE dbo.tTitles.tconst = dbo.tGenres.tconst AND dbo.tTitles.tconst IN (SELECT tconst FROM dbo.tPrincipals WHERE nconst = " + str(actor) + " AND category = 'acted in' INTERSECT SELECT tconst FROM dbo.tPrincipals WHERE nconst = " + str(director) " AND category = 'directed')"
                    else:
                        myRequest = "SELECT AVG(runtimeMinutes) FROM dbo.tTitles, dbo.tGenres WHERE dbo.tTitles.tconst = dbo.tGenres.tconst AND dbo.tTitles.tconst IN (SELECT tconst FROM dbo.tPrincipals WHERE nconst = " + str(actor) + " AND category = 'acted in')"
                else:
                    myRequest = "SELECT AVG(runtimeMinutes) FROM dbo.tTitles, dbo.tGenres WHERE dbo.tTitles.tconst = dbo.tGenres.tconst AND dbo.tTitles.tconst IN (SELECT tconst FROM dbo.tPrincipals WHERE nconst = " + str(director) " AND category = 'directed')"
            cursor.execute(myRequest)
            rows = cursor.fetchall()
            for row in rows:
                dataString += f"SQL: Durée_Moyenne={row[0]}\n"

    except:
        errorMessage = "Erreur de connexion a la base SQL"

    if errorMessage != "":
        return func.HttpResponse(dataString + errorMessage, status_code=500)

    else:
        return func.HttpResponse(dataString + " Connexions réussies à SQL!")
