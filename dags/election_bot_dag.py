from cgitb import html
from unicodedata import name
import airflow
from selenium import webdriver
import subprocess
import os
import time 
import json
import imgkit
import folium
import datetime
import urllib.request as request
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=0.5),
}

election_bot_dag = DAG(
    dag_id='election_bot_dag',
    default_args=default_args_dict,
    catchup=False,
)

get_data = BashOperator(
    task_id='get_spreadsheet',
    dag=election_bot_dag,
    bash_command="curl -s https://presidentielle2022.conseil-constitutionnel.fr/telechargement/parrainagestotal.csv --output /opt/airflow/dags/ressources/bin/parrainage.csv",
)

# create the query for postgre
def _create_data_query(output_folder: str, ds_nodash: str):
    df = pd.read_csv(f"{output_folder}/ressources/bin/parrainage.csv", delimiter=";")
    with open(f"{output_folder}/query/parrainage_query.sql", "w") as f:
        df_iterable = df.iterrows()
        f.write(
            "DROP TABLE parrainage;\n"
            "CREATE TABLE IF NOT EXISTS parrainage (\n"
            "civilite VARCHAR(255),\n"
            "nom VARCHAR(255),\n"
            "prenom VARCHAR(255),\n"
            "mandat VARCHAR(255),\n"
            "circonscription VARCHAR(255),\n"
            "departement VARCHAR(255),\n"
            "candidat VARCHAR(255),\n"
            "data_publication VARCHAR(255));\n"
        )
        for index, row in df_iterable:
            civilite = row['Civilité']
            nom = str(row['Nom']).replace("'",'$')
            prenom = str(row['Prénom']).replace("'",'$')
            mandat = str(row['Mandat']).replace("'",'$')
            circonscription = str(row['Circonscription']).replace("'",'$')
            departement = str(row['Département']).replace("'",'$')
            candidat = str(row['Candidat']).replace("'",'$')
            date_publication = row['Date de publication']
            f.write(
                'INSERT INTO parrainage ("civilite", "nom", "prenom", "mandat", "circonscription", "departement", "candidat", "data_publication") VALUES ('
                f"'{civilite}', '{nom}', '{prenom}', '{mandat}', '{circonscription}', '{departement}', '{candidat}', '{date_publication}'"
                ");\n"
            )

        f.close()

create_data_query = PythonOperator(
    task_id='create_data_query',
    dag=election_bot_dag,
    python_callable=_create_data_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
        'ds_nodash': '{{ds_nodash}}'
    },
    trigger_rule='all_success',
)

insert_data_query = PostgresOperator(
    task_id='insert_data_query',
    dag=election_bot_dag,
    postgres_conn_id='postgres_default',
    sql='query/parrainage_query.sql',
    trigger_rule='all_success',
    autocommit=True,
)

def _map(output_folder: str, ds_nodash: str):
    [print(item) for item in os.environ['PATH'].split(';')]
    print(os.listdir("/usr/bin"))
    cap = DesiredCapabilities().FIREFOX
    cap["marionette"] = False
    driver = webdriver.Firefox(capabilities=cap)
   

    departement_geo = f"{output_folder}/ressources/departements-version-simplifiee.geojson"
    parrainage = f"{output_folder}/ressources/bin/parrainage.csv"   
    parrainage_data = pd.read_csv(parrainage, delimiter=";")
    with open(f"{output_folder}/ressources/liste_candidat.json") as json_file:
        candidat_data = json.load(json_file)
    print(candidat_data)

    grouped_parrainage_data = parrainage_data.groupby(["Département","Candidat"]).count().filter(["Département", "Candidat", "Nom"]).rename(columns={'Nom':'Count'})
    grouped_parrainage_data = grouped_parrainage_data.reset_index()

    for candidat in candidat_data["candidats"]:
        denomination = candidat["denomination"]
        filtered_data = grouped_parrainage_data[grouped_parrainage_data['Candidat'] == denomination]

        m = folium.Map(location=[46, 2], tiles="cartodbpositron", zoom_start=5.5)
        folium.Choropleth(
            geo_data=departement_geo,
            data=filtered_data,
            columns=["Département", "Count"],
            name="Parrainage",
            key_on="feature.properties.nom"
        ).add_to(m)

        htmlURL = f"{output_folder}/ressources/bin/{denomination}.html"
        m.save(htmlURL)

        driver.get(htmlURL)

        time.sleep(5)

        driver.save_screenshot(f'{output_folder}/ressources/bin/{denomination}.png')

        
        #bshCmd = ["cutycapt", f"--url={htmlURL}", f"--out={output_folder}/ressources/bin/{denomination}.png"] 
        option = {
            "--javascript-delay" : "5000",
            "--crop-h":"900",
            "--crop-w":"900",
            "--crop-x":"0",            
            "--crop-y":"0",
        }
        options = {
            "--javascript-delay" : "1000",
            'width': 400,
            "width": 400
        }
        #imgkit.from_file(htmlURL,f"{output_folder}/ressources/bin/{denomination}.png",options=options)
        #process = subprocess.Popen(bshCmd, stdout=subprocess.PIPE)
    driver.quit()
map = PythonOperator(
    task_id='map',
    dag=election_bot_dag,
    python_callable=_map,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
        'ds_nodash': '{{ds_nodash}}'
    },
    trigger_rule='all_success',
)

get_data >> create_data_query >>  insert_data_query >> map