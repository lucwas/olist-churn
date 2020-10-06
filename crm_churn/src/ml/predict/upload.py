import pandas as pd
import os
import datetime

import sqlalchemy
from pyspark.sql import SparkSession

ML_PREDICT_DIR = os.path.dirname(os.path.abspath(__file__))
ML_DIR = os.path.dirname(ML_PREDICT_DIR)
SRC_DIR = os.path.dirname(ML_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(BASE_DIR, 'data')
DATA_PREDICT_DIR = os.path.join(DATA_DIR, 'predict')
DATA_SCORE_DIR = os.path.join(DATA_DIR, 'score')
MODEL_DIR = os.path.join(BASE_DIR, 'models')

print("Importando os dados com score...", end="")
df = pd.read_csv(os.path.join(DATA_SCORE_DIR, "tb_score.csv"),
                sep = "|",
                index_col=[0])
print("ok.")

print("Abrindo conexão com banco de dados...", end="")
DATABASE_DIR = os.path.abspath(os.path.join(__file__, "../../../../.."))
spark = sqlalchemy.create_engine("sqlite:///" + str(DATABASE_DIR) +"\\database\\olist.db")
print("ok.")


print(df.head())

# Evitando que haja duplicatas de previsoes feitas no mesmo dia
try:
    query = sqlalchemy.text("DELETE FROM tb_score_churn   WHERE dt_ref IN :ids;")
    query = query.bindparams(sqlalchemy.bindparam('ids', expanding=True))
    spark.execute(query, ids=list(df['dt_ref'].unique()))
except:
    print('tabela vazia/primeiro dado do dia')

print("Salvando dataframe no banco de dados...", end="\n")
df.to_sql("tb_score_churn", spark, if_exists="append")
print("ok.")
