import pandas as pd 
import numpy as np 
from unir_inconsistencias import unir_arquivos
from dimensoes_metricas import abri_arquivo_classificado
nome = '/base1_inconsistencias_unido.csv'
diretorio = 'inconsistencias'

def dimensao_busca(nome_inconsistencia):
    dimensao_dicionario =  {
    'vazio'  :  'Completude',
    'X'  :  'Conformidade/Exatidão',
    'caracter_especial'  :  'Conformidade',
    'caracter_especial_1'  :  'Conformidade/Exatidão',
    'dt_cadastro_aluno_sistema_inferior' :'Consistência',
    'dt_cadastro_ciclo_inferior': 'Consistência',
    'tipo_oferta_diferente_hifen': 'Consistência'
    }
    return dimensao_dicionario[nome_inconsistencia]

def classfica_dimensoes(df_dados):
    print("classificando inconsistências para adicionar a dimensão afetada.")
    dimensao = []
    for index, linha in df_dados.iterrows():
            nome_inconsistencia = linha.tipo_inconsistencia
            dimensao.append(dimensao_busca(nome_inconsistencia))
    df_dados['Dimensão'] = dimensao
    salvar_classificacao(df_dados)

def salvar_classificacao(df_dados):
    df_dados.to_csv(diretorio + '/base1_inconsistencias_unido_classificado.csv', sep=',', encoding="utf-8", index=False)
    print('arquivo salvo com a dimensão classificada')


if __name__ == "__main__":
    # unir_arquivos()
    # df_dados = pd.read_csv('inconsistencias/base1_inconsistencias_unido.csv', sep=',',encoding='utf-8')
    # classfica_dimensoes(df_dados)
    abri_arquivo_classificado()
    
