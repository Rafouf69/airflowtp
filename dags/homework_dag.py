from unicodedata import name
import airflow
import os
import random
import json
import datetime
import urllib.request as request
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from faker import Faker 

default_args_dict = {
    'start_date': airflow.utils.dates.days_ago(0),
    'concurrency': 1,
    'schedule_interval': '@daily',
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=0.5),
}

homework_dag = DAG(
    dag_id='homework_dag',
    default_args=default_args_dict,
    catchup=False,
)

#function to get number of character
def getCharactersNumber(output_folder):
    f = open(f"{output_folder}/character_data.json", "r")
    dict = json.loads(f.read())
    f.close()
    return len(dict)

#function to read and write in json
def read_write_json(output_folder, stringToReturn, key):
    f = open(f"{output_folder}/character_data.json", "r")
    dict = json.loads(f.read())
    f.close()

    dict[getCharactersNumber(output_folder)-1][key] = stringToReturn
    
    f = open(f"{output_folder}/character_data.json", "w")
    f.write(json.dumps(dict))
    f.close()

#function to generate a random number
def random_numbers(output_folder, min, max, number):
    request.urlretrieve(url=f"http://www.randomnumberapi.com/api/v1.0/random?min={min}&max={max}&count={number}", filename=f"{output_folder}/number.txt")
    f = open(f"{output_folder}/number.txt", "r")
    list = f.read().strip('[]').split(",")
    numberList = []
    for i in range(len(list)):
        numberList.append(int(list[i]))
    f.close()
    return numberList

# check if file exist
def _beginning(output_folder):
    if(os.path.isfile(f"{output_folder}/character_data.json")):
        return 'append_character'
    else:
        return 'create_file'


beginning = BranchPythonOperator(
    task_id='beginning',
    dag=homework_dag,
    python_callable=_beginning,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# file does exist 

def _append_character(output_folder):
    f = open(f"{output_folder}/character_data.json", "r")
    dict = json.loads(f.read())
    f.close()

    dict.append({})
    
    f = open(f"{output_folder}/character_data.json", "w")
    f.write(json.dumps(dict))
    f.close()


task_zero_a = PythonOperator(
    task_id='append_character',
    dag=homework_dag,
    python_callable=_append_character,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# file does not exist 

def _create_file(output_folder):
    f = open(f"{output_folder}/character_data.json", "w")
    dict= [{}]
    f.write(json.dumps(dict))
    f.close()


task_zero_b = PythonOperator(
    task_id='create_file',
    dag=homework_dag,
    python_callable=_create_file,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# Creating a new random name
def _generate_name(output_folder):
    fake = Faker()
    fake = fake.name()
    read_write_json(output_folder, fake, "name")


task_one = PythonOperator(
    task_id='generate_name',
    dag=homework_dag,
    python_callable=_generate_name,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='one_success',
    depends_on_past=False,
)

# generate attribute with the 15, 14, 13, 12, 10, 8 rule
def _generate_attribute(output_folder):
    list = [15,14,13,12,10,8]
    
    listToReturn = []

    for i in range (len(list)):
        if(len(list)-1 != 0):
            index = random_numbers(output_folder,0,len(list)-1,1)[0]
        else:
            index=0
        value = list[index]
        list.remove(value)
        listToReturn.append(value)

    read_write_json(output_folder, str(listToReturn), "attributes")


task_two = PythonOperator(
    task_id='generate_attribute',
    dag=homework_dag,
    python_callable=_generate_attribute,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# get the race from an API

def _get_race(epoch, url, output_folder):
    request.urlretrieve(url=url, filename=f"{output_folder}/{epoch}.json")
    f = open(f"{output_folder}/{epoch}.json", "r")
    jsonObject = json.loads(f.read())
    f.close()

    index = random_numbers(output_folder,0,jsonObject["count"]-1,1)[0]

    object = jsonObject["results"][index]

    read_write_json(output_folder, object["index"], "race")

task_three = PythonOperator(
    task_id='get_race',
    dag=homework_dag,
    python_callable=_get_race,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "race",
        "url": "https://www.dnd5eapi.co/api/races"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# get the class from an API

def _get_class(epoch, url, output_folder):
    request.urlretrieve(url=url, filename=f"{output_folder}/{epoch}.json")
    f = open(f"{output_folder}/{epoch}.json", "r")
    jsonObject = json.loads(f.read())
    f.close()

    index = random_numbers(output_folder,0,jsonObject["count"]-1,1)[0]

    object = jsonObject["results"][index]

    read_write_json(output_folder, object["index"], "class")

task_four = PythonOperator(
    task_id='get_class',
    dag=homework_dag,
    python_callable=_get_class,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "class",
        "url": "https://www.dnd5eapi.co/api/classes"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# get the proficiencies from an API

def _get_proficiencies(epoch, url, output_folder):
    f = open(f"{output_folder}/character_data.json", "r")
    jsonObject = json.loads(f.read())
    f.close()

    myClass = jsonObject[getCharactersNumber(output_folder)-1]["class"]

    request.urlretrieve(url=f"{url}/{myClass}/proficiencies", filename=f"{output_folder}/{epoch}.json")

    f = open(f"{output_folder}/{epoch}.json", "r")
    jsonObject = json.loads(f.read())
    f.close()

    proficienciesString = "["

    for i in range( jsonObject["count"]):
        proficienciesString+=jsonObject["results"][i]["index"]+", "

    proficienciesString += "]"

    read_write_json(output_folder, proficienciesString, "proficiencies")

task_five = PythonOperator(
    task_id='get_proficiencies',
    dag=homework_dag,
    python_callable=_get_proficiencies,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "proficiencies",
        "url": "https://www.dnd5eapi.co/api/classes"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# generate the level of the character


def _generate_level(output_folder):
    level = random_numbers(output_folder,1,3,1)[0]

    read_write_json(output_folder, str(level), "level")

task_six = PythonOperator(
    task_id='generate_level',
    dag=homework_dag,
    python_callable=_generate_level,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# get the skills from an API

def _get_spells(epoch, url, output_folder):
    f = open(f"{output_folder}/character_data.json", "r")
    jsonObject = json.loads(f.read())
    f.close()

    myClass = jsonObject[getCharactersNumber(output_folder)-1]["class"]
    myLevel = jsonObject[getCharactersNumber(output_folder)-1]["level"]
    request.urlretrieve(url=f"{url}/{myClass}/spells?level={myLevel}", filename=f"{output_folder}/{epoch}.json")

    f = open(f"{output_folder}/{epoch}.json", "r")
    jsonObject = json.loads(f.read())
    f.close()

    spellString = "["

    if(jsonObject["count"]>0):
        for i in range(int(myLevel)+3):
            index = random_numbers(output_folder, 0, jsonObject["count"]-1, 1)[0]
            print(index)
            print(jsonObject["count"])
            spellString+=jsonObject["results"][index]["index"]+", "

    spellString += "]"

    read_write_json(output_folder, spellString, "spells")

task_seven = PythonOperator(
    task_id='get_spells',
    dag=homework_dag,
    python_callable=_get_spells,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "spells",
        "url": "https://www.dnd5eapi.co/api/classes"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# get the languages

def _get_languages(epoch, url, output_folder):
    f = open(f"{output_folder}/character_data.json", "r")
    jsonObject = json.loads(f.read())
    f.close()

    myRace = jsonObject[getCharactersNumber(output_folder)-1]["race"]

    request.urlretrieve(url=f"{url}/{myRace}", filename=f"{output_folder}/{epoch}.json")

    f = open(f"{output_folder}/{epoch}.json", "r")
    languageList = json.loads(f.read())["languages"]
    f.close()

    languageString = "["

    for i in range(len(languageList)):
        languageString+=languageList[i]["index"]+", "

    languageString += "]"

    read_write_json(output_folder, languageString, "languages")

task_eight = PythonOperator(
    task_id='get_languages',
    dag=homework_dag,
    python_callable=_get_languages,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
        "epoch": "language",
        "url": "https://www.dnd5eapi.co/api/races"
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

join_tasks_one = DummyOperator(
    task_id='coalesce_transformations_one',
    dag=homework_dag,
    trigger_rule='none_failed'
)

join_tasks_two = DummyOperator(
    task_id='coalesce_transformations_two',
    dag=homework_dag,
    trigger_rule='none_failed'
)


# check if file exist
def _is_enough(output_folder):
    if(getCharactersNumber(output_folder)<5):
        return 'end'
    else:
        return 'create_character_query'


is_enough = BranchPythonOperator(
    task_id='is_enough',
    dag=homework_dag,
    python_callable=_is_enough,
    op_kwargs={
        "output_folder": "/opt/airflow/dags",
    },
    trigger_rule='all_success',
    depends_on_past=False,
)

# create the query for postgre
def _create_character_query(output_folder: str):
    f = open(f"{output_folder}/character_data.json", "r")
    dict = json.loads(f.read())
    f.close()
    with open(f"{output_folder}/character_inserts.sql", "w") as f:
        f.write(
            "DROP TABLE character;"
            "CREATE TABLE IF NOT EXISTS character (\n"
            "name VARCHAR(255),\n"
            "attributes VARCHAR(255),\n"
            "race VARCHAR(255),\n"
            "languages VARCHAR(255),\n"
            "class VARCHAR(255),\n"
            "profficiency_choices VARCHAR(255),\n"
            "level VARCHAR(255),\n"
            "spells VARCHAR(255));\n"
        )
        for index in range(len(dict)):
            name = dict[index]["name"]
            attributes = dict[index]["attributes"]
            race = dict[index]["race"]
            languages = dict[index]["languages"]
            myClass = dict[index]["class"]
            profficiency_choices =  dict[index]["proficiencies"]
            level = dict[index]["level"]
            spells = dict[index]["spells"]

            f.write(
                "INSERT INTO character VALUES ("
                f"'{name}', '{attributes}', '{race}', '{languages}', '{myClass}', '{profficiency_choices}', '{level}', '{spells}'"
                ");\n"
            )


task_nine = PythonOperator(
    task_id='create_character_query',
    dag=homework_dag,
    python_callable=_create_character_query,
    op_kwargs={
        'previous_epoch': '{{ prev_execution_date.int_timestamp }}',
        'output_folder': '/opt/airflow/dags',
    },
    trigger_rule='all_success',
)

task_ten = PostgresOperator(
    task_id='insert_character_query',
    dag=homework_dag,
    postgres_conn_id='postgres_default',
    sql='character_inserts.sql',
    trigger_rule='all_success',
    autocommit=True,
)

task_eleven = BashOperator(
    task_id='cleanup',
    dag=homework_dag,
    bash_command="rm /opt/airflow/dags/*.sql /opt/airflow/dags/*.json /opt/airflow/dags/*.txt",
)

end = DummyOperator(
    task_id='end',
    dag=homework_dag,
    trigger_rule='none_failed',
)


beginning >> [task_zero_a, task_zero_b]
[task_zero_a, task_zero_b] >> task_one
task_one >> [task_two, task_three, task_four, task_six]
task_three >> task_eight
task_four >> task_five
[task_five, task_six] >> join_tasks_two
join_tasks_two >> task_seven
[task_one, task_two, task_eight, task_seven] >> join_tasks_one
join_tasks_one >> is_enough
is_enough >> [end, task_nine]
task_nine >> task_ten
task_ten >> task_eleven 
task_eleven >> end