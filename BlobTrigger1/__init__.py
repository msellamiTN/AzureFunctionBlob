import logging
import pandas as pd
import azure.functions as func
import os, io
import csv
import json    
from cassandra.auth import PlainTextAuthProvider
import urllib.request
from cassandra.query import BatchStatement, SimpleStatement
 
import json
import urllib.request
import time
import ssl
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.policies import *
from ssl import PROTOCOL_TLSv1_2, SSLContext, CERT_NONE
from requests.utils import DEFAULT_CA_BUNDLE_PATH 
import os
    
config = {
         'username': 'fitec-account-cosmos-232259431',

         'password': 'LOYdNuQhgT8yz0xAuxScaigrxnvBZCRwkyEi8tRyjOJuNpzPCxDxMNKpUmDUlnvNC626iflPuCq5hCQgL2xzrA==',

         'contactPoint': 'fitec-account-cosmos-232259431.cassandra.cosmos.azure.com',

         'port':'10350'
         }
def ConnectCassandra():
    #<authenticateAndConnect>
    ssl_context = SSLContext(PROTOCOL_TLSv1_2)
    ssl_context.verify_mode = CERT_NONE
    auth_provider = PlainTextAuthProvider(username=config['username'], password=config['password'])
    cluster = Cluster([config['contactPoint']], port = config['port'], auth_provider=auth_provider,ssl_context=ssl_context)
    session = cluster.connect()
    #<createKeyspace>
    session.execute("CREATE KEYSPACE IF NOT EXISTS Covid WITH REPLICATION={'class':'SimpleStrategy','replication_factor':1};")

    session.execute("USE Covid;")


    session.execute("CREATE TYPE IF NOT EXISTS Covid.CovidType (confirmed INT,deaths INT,recovred INT,active INT);")


    session.execute("CREATE TABLE IF NOT EXISTS Covid.countries ("\
    "country TEXT,date Text,longitude Text,latitude Text,covid map<TEXT,frozen<CovidType>>,primary key (country ));")

    #</authenticateAndConnect>
    return session 
def format_date(date):
    
    formatted_date = datetime.strptime(date,'%m/%d/%Y')
    return formatted_date.strftime('%Y-%m-%d')

def main(fitecdata: func.InputStream):
    logging.info(f"Python blob trigger function processed blob \n"
                 f"Name: {fitecdata.name}\n"
                 f"Blob Size: {fitecdata.length} bytes")
    #connection a cassandra 
    session=ConnectCassandra()
    # Load in the tech and crm data
    file = fitecdata.read()
    logging.info (type(file))
    confirmed = pd.read_csv(io.BytesIO(file), sep=',', dtype=str).groupby(['Country/Region'],as_index=False).sum()
    logging.info (len(confirmed))
    
    df_con_melted = pd.melt(confirmed, id_vars=['Province/State','Country/Region','Lat','Long'], var_name="Date",value_name="Confirmed")
    for index,row in df_con_melted.iterrows():
            logging.info(row)

    # On stocke les données de la ligne une par une
    # On remplace le caractère "'" par un espace pour éviter des erreurs de parsing de la query
            country = row["Country/Region"].replace("'"," ")
            country.replace("'"," ")
            date =(row["Date"])
             
            long = row["Long"]
            lat = row["Lat"]
            confirmed = row["Confirmed"]
            deaths =0
            recovered = 0
            active = 0
    
    # On crée la query
            query = "INSERT INTO Covid.countries(country,date,longitude,latitude,covid) VALUES ("\
            "'{}','{}','{}','{}',{{'{}':"\
            "{{confirmed : {},deaths : {}, recovred : {}, active : {} }}}});"\
            .format(country,date,long,lat,date,confirmed,deaths,recovered,active)                         
            logging.info(query)
            #On effectue la query sur la base
            session.execute(query)
            #stream
     
    
      
 