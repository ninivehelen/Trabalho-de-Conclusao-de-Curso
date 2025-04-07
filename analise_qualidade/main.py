import pandas as pd 
import numpy as np 
from unir_inconsistencias import unir_arquivos
nome = '/base1_inconsistencias_unido.csv'
diretorio = 'inconsistencias'

def dimensao_busca(nome_inconsistencia):
    dimensao_dicionario =  {
    'vazio'  :  'Completude',
    'X'  :  'Conformidade',
    'X'  :  'Exatidão',
    'caracter_especial'  :  'Conformidade',
    'dt_cadastro_aluno_sistema_inferior' :'Consistência',
    'dt_cadastro_ciclo_inferior': 'Consistência',
    'tipo_oferta_diferente_hifen': 'Consistência'
    }
    return dimensao_dicionario[nome_inconsistencia]

def classfica_dimensoes_exatidao(df_dados):
    lista_linhas = ['sg_sexo']
    print("classificando inconsistências para adicionar a dimensão afetada.")
    dimensao = []
    for index, linha in df_dados.iterrows():
        if linha.coluna in lista_linhas:
            nome_inconsistencia = linha.tipo_inconsistencia
            dimensao.append(dimensao_busca(nome_inconsistencia))
    df_dados['Dimensão'] = dimensao
    df_dados.to_csv(diretorio + '/base1_inconsistencias_unido_classificado.csv', sep=',', encoding="utf-8", index=False)
    print('arquivo salvo com a dimensão classificada')

def classfica_dimensoes_completude(df_dados):
    lista_linhas = ['sg_sexo']
    print("classificando inconsistências para adicionar a dimensão afetada.")
    dimensao = []
    for index, linha in df_dados.iterrows():
        if linha.coluna in lista_linhas:
            nome_inconsistencia = linha.tipo_inconsistencia
            dimensao.append(dimensao_busca(nome_inconsistencia))
    df_dados['Dimensão'] = dimensao
    salvar_classificacao(df_dados)
   
def salvar_classificacao(df_dados):
    df_dados.to_csv(diretorio + '/base1_inconsistencias_unido_classificado.csv', sep=',', encoding="utf-8", index=False)
    print('arquivo salvo com a dimensão classificada')


if __name__ == "__main__":
     df_dados = pd.read_csv('inconsistencias/base_dados_1_valida_sexo_aluno_sg_sexo_07_04_2025-15_56.csv', sep=',',encoding='utf-8')
     df_dados = df_dados.replace(np.nan, 'vazio')
     total_linhas_dados = df_dados.shape[0]
     classfica_dimensoes_completude(df_dados)
    
