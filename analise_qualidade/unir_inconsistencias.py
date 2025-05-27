import glob
import os 
import pandas as pd 
import numpy as np 
diretorio = 'inconsistencias'

def unir_arquivos():
    arquivos_csv = glob.glob(os.path.join(diretorio+ "/base_dados_1*.csv")) 
    print(arquivos_csv)
    base_dados = pd.DataFrame()
    for arquivo in arquivos_csv:
        df_temp = pd.read_csv(arquivo, encoding='utf-8')
        base_dados  = pd.concat([df_temp,base_dados])
    df_dados_ = base_dados.replace(np.nan, 'vazio')
    df_dados_.to_csv(diretorio+'/base1_inconsistencias_unido.csv', index=False)
    print("CSV com todas as inconsistências criado")
