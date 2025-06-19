import pandas as pd 
import numpy as np 
from unir_inconsistencias import unir_arquivos
from dimensoes_metricas import *

nome = '/base1_inconsistencias_unido.csv'
diretorio = 'inconsistencias'

# este dicionario ajuda a adicionar qual dimensão foia afetada de acordo com a descrição da inconsistência.
def dimensao_busca(nome_inconsistencia):
    dimensao_dicionario =  {
    'vazio'  :  'Completude',
    'tipo_não_valido'  :  'Conformidade/Exatidão',
    'caracter_especial'  :  'Conformidade/Exatidão',
    'estado_uma_sigla_não_valida': 'Comformidade/Exatidão',
    'valor_diferente_F_M': 'Comformidade/Exatidão',
    'cnpj_invalido' :'Comformidade/Exatidão',
    'não_conforme_a_regra': 'Consistência',
    'diferente_hifen_e_não_conforme_a_regra': 'Consistência',
    'vazio_não_conforme_a_regra': 'Completude/Consistência',
    'diferente_dos_eixos_e_não_comforme_a_regra': 'Comformidade/Exatidão/Consistência',
    'boleano_invalido': 'Comformidade/Exatidão',
    'boleano_invalido_lista_cota':'Comformidade/Exatidão',
    'data_invalida':'Comformidade/Exatidão',
    'dt_cadastro_ciclo_inferior_não_conforme_a_regra':'Consistência',
    'dt_deferimento_ue_superior_não_conforme_a_regra':'Consistência',
    'dt_cadastro_aluno_sistema_inferior_não_conforme_a_regra':'Consistência',
    'ano_invalido':'Comformidade/Exatidão',
    'ano_maior_que_o_ano_atual_não_conforme_a_regra':'Consistência',
    'dt_geracao_existe_dt_validacao_não_existe_não_conforme_a_regra':'Completude/Consistência',
    }
    return dimensao_dicionario[nome_inconsistencia]

def classfica_dimensoes(df_dados):
    print("classificando inconsistências para adicionar a dimensão afetada.")
    dimensao = []
    for index, linha in df_dados.iterrows():
        nome_inconsistencia = linha['tipo_inconsistencia']
        dimensao.append(dimensao_busca(nome_inconsistencia))
    df_dados['Dimensão'] = dimensao
    salvar_classificacao(df_dados)

def salvar_classificacao(df_dados):
    df_dados.to_csv(diretorio + '/base1_inconsistencias_unido_classificado.csv', index=False)
    print('arquivo salvo com a dimensão classificada')

if __name__ == "__main__":
    unir_arquivos()
    df_dados = pd.read_csv('inconsistencias/base1_inconsistencias_unido.csv')
    classfica_dimensoes(df_dados)
    df_dados_classificados, total_df_sistec = abri_arquivo_classificado()
    metrica_completude(df_dados_classificados, total_df_sistec)
    metrica_consistencia(df_dados_classificados,total_df_sistec)
    metrica_exatidao(df_dados_classificados,total_df_sistec)
    
