## acessa o cluster e baixa arquivo para a maquina local

import pandas as pd
import os
from pyspark.sql import SparkSession
import sqlalchemy
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--date", "-d", default = '2018-01-01', type=str, help="Data reference")
args = parser.parse_args()


DATA_PREP_PRED_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PREP_DIR = os.path.dirname(DATA_PREP_PRED_DIR)
SRC_DIR = os.path.dirname(DATA_PREP_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join( BASE_DIR, 'data' )
DATA_TRAIN_DIR = os.path.join( DATA_DIR, 'train' )

if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR)

if not os.path.exists(DATA_PREP_DIR):
    os.mkdir(DATA_PREP_DIR)

#Imposrtando a query
print("Importando query...")
with open(os.path.join(DATA_PREP_PRED_DIR, 'etl.sql'), 'r') as open_file:
    query = open_file.read()
query = query.format(dt_ref = args.date)
print("ok.")

print("Abrindo conexão com o spark...", end="")
#spark = SparkSession.builder.getOrCreate()
DATABASE_DIR = os.path.abspath(os.path.join(__file__, "../../../../.."))
spark = sqlalchemy.create_engine("sqlite:///" + str(DATABASE_DIR) +"\\database\\olist.db")
print("ok.")


df_predict = pd.read_sql(query, spark)
print("\n")
print(df_predict.head())
df_predict.to_csv( os.path.join(DATA_DIR,'predict', 'tb_predict.csv'),
               index=False,
               sep = "|")
print("ok.")