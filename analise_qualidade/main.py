import pandas as pd 
import numpy as np 
import glob
import os 
from dimensoes_metricas import abri_arquivo_classificado
nome = '/base1_inconsistencias_unido.csv'
diretorio = 'inconsistencias'

def unir_arquivos():
    arquivos_csv = glob.glob(os.path.join(diretorio+ "/base_dados_1_*.csv")) 
    base_dados = pd.DataFrame()
    for arquivo in arquivos_csv:
        df_temp = pd.read_csv(arquivo, sep=',',encoding='utf-8', keep_default_na = False)
        base_dados = pd.concat([df_temp,base_dados])
        print("Unindo csv")
        base_dados.to_csv(diretorio+'/base1_inconsistencias_unido.csv', sep=',',encoding="utf-8",index=False)
    print("CSV com todas as inconsistências criado")

def dimensao_busca(nome_inconsistencia):
    dimensao_dicionario =  {
    'vazio'  :  'Completude',
    'X'  :  'Conformidade',
    'caracter_especial'  :  'Conformidade',
    'dt_cadastro_aluno_sistema_inferior' :'Consistência',
    'dt_cadastro_ciclo_inferior': 'Consistência',
    'tipo_oferta_diferente_hifen': 'Consistência'
    }
    return dimensao_dicionario[nome_inconsistencia]

def classfica_dimensoes_dados(df_dados):
    print("Classificando as inconsistências, adicionando as dimensoes afetadas")
    dimensão = []
    for index, linha in df_dados.iterrows():
        nome_inconsistencia = linha.tipo_inconsistencia
        dimensão.append(dimensao_busca(nome_inconsistencia))
    df_dados['Dimensão'] = dimensão
    df_dados.to_csv(diretorio+'/base1_inconsistencias_unido_classificado.csv', sep=',',encoding="utf-8",index=False)
    print('Arquivo com as dimensões salvo')

if __name__ == "__main__":
     unir_arquivos()
     df_dados = pd.read_csv(diretorio+"/base1_inconsistencias_unido.csv", sep=',',encoding="utf-8")
     df_dados = df_dados.replace(np.nan, 'vazio')
     total_linhas_dados = df_dados.shape[0]
     classfica_dimensoes_dados(df_dados)
     nome_arquivo = pd.read_csv(diretorio+'/base1_inconsistencias_unido_classificado.csv', sep=',',encoding="utf-8")
     abri_arquivo_classificado(nome_arquivo)
