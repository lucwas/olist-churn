import pandas as pd
import os
import datetime


ML_PREDICT_DIR = os.path.dirname(os.path.abspath(__file__))
ML_DIR = os.path.dirname(ML_PREDICT_DIR)
SRC_DIR = os.path.dirname(ML_DIR)
BASE_DIR = os.path.dirname(SRC_DIR)
DATA_DIR = os.path.join(BASE_DIR, 'data')
DATA_PREDICT_DIR = os.path.join(DATA_DIR, 'predict')
DATA_SCORE_DIR = os.path.join(DATA_DIR, 'score')
MODEL_DIR = os.path.join(BASE_DIR, 'models')

print("Importando modelo...", end="")
model = pd.read_pickle(os.path.join(MODEL_DIR, "model_churn.pkl"))
print("ok...")

print("Importando a base de dados...", end="")
df = pd.read_csv(os.path.join(DATA_PREDICT_DIR, "tb_predict.csv"),  
                                sep="|",
                                usecols=model['fit_vars']+['dt_ref', 'seller_id'])
print("ok...")


tb_score = model['model'].predict_proba(df[model['fit_vars']])
df_score = pd.DataFrame({'seller_id': df['seller_id'],
                        'proba_churn': tb_score[:,1]})
df_score['dt_ref'] = datetime.date.today().strftime("%Y-%m-%d") 

print(df_score.head())

if not os.path.exists(DATA_SCORE_DIR):
    os.mkdir(DATA_SCORE_DIR)

print("salvando a base com scores...", end="")
df_score.to_csv(os.path.join(DATA_SCORE_DIR, "tb_score.csv"), 
                sep = "|",
                index = False)
print("ok.")