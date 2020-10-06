## acessa o cluster e baixa arquivo para a maquina local

import pandas as pd
import os
from pyspark.sql import SparkSession
import sqlalchemy

DATA_PREP_TRAIN_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PREP_DIR = os.path.dirname(DATA_PREP_TRAIN_DIR)
SRC_DIR = os.path.dirname(DATA_PREP_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join( BASE_DIR, 'data' )
DATA_TRAIN_DIR = os.path.join( DATA_DIR, 'train' )

if not os.path.exists(DATA_DIR):
    os.mkdir(DATA_DIR)

if not os.path.exists(DATA_TRAIN_DIR):
    os.mkdir(DATA_TRAIN_DIR)

print("Abrindo conexão com o spark...", end="")
#spark = SparkSession.builder.getOrCreate()
DATABASE_DIR = os.path.abspath(os.path.join(__file__, "../../../../.."))
print(DATABASE_DIR)
spark = sqlalchemy.create_engine("sqlite:///" + str(DATABASE_DIR) +"\\database\\olist.db")
print("ok.")

print("Coletando ABT do datalake...", end="")
#df_abt = spark.table("tb_abt_churn").toPandas()
df_abt = pd.read_sql('select * from tb_abt_churn', spark)
print("\n")
print(df_abt.head())
df_abt.to_csv( os.path.join(DATA_DIR, 'train', 'abt_churn.csv'),
               index=False,
               sep="|" )
print("ok.")