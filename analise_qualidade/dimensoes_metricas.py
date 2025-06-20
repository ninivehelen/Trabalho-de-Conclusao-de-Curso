import pandas as pd 
import os 

def abri_arquivo_classificado():
    df_dados_classificados = pd.read_csv('inconsistencias/base1_inconsistencias_unido_classificado.csv', sep=',',encoding='utf-8')
    df_sistec = pd.read_csv('sistec_ifb.csv', sep=';',encoding='utf-8')
    total_linhas_dados = df_dados_classificados.shape[0]
    total_df_sistec = df_sistec.shape[0]
    print('Total de linhas dados Sistec IFB', total_df_sistec)
    return(df_dados_classificados, total_df_sistec)

def metrica_exatidao(df_dados_classificados,total_df_sistec):
    #Metrica de exatidao
    exatidao = df_dados_classificados.loc[df_dados_classificados['Dimensão'].isin(['Conformidade/Exatidão', 'Comformidade/Exatidão/Consistência'])]
    exatidao_total_linhas = exatidao.shape[0]
    print('Total de inconsistencia de exatidão', exatidao_total_linhas)
    total_linhas_diferenca = (total_df_sistec - exatidao_total_linhas) 
    print('Diferença de total inconsistencia/ Total linhas dataframe', total_linhas_diferenca)
    porcentagem_qualidade = (total_linhas_diferenca / total_df_sistec) * 100
    porcenta_duas_decimais  = "{:.3f}".format(porcentagem_qualidade)
    print(porcenta_duas_decimais)
    salvar_dados_metrica('Exatidão', porcenta_duas_decimais)
  
def metrica_completude(df_dados_classificados,total_df_sistec):
    #Metrica de completude
    completude = df_dados_classificados[df_dados_classificados['Dimensão'] == 'Completude']
    completude_total_linhas = completude.shape[0]
    print('Total de inconsistencia de completude', completude_total_linhas)
    total_linhas_diferenca = (total_df_sistec - completude_total_linhas) 
    print('Diferença de total inconsistencia/ Total linhas dataframe', total_linhas_diferenca)
    porcentagem_qualidade = (total_linhas_diferenca / total_df_sistec) * 100
    porcenta_duas_decimais  = "{:.3f}".format(porcentagem_qualidade)
    print(porcenta_duas_decimais)
    salvar_dados_metrica('Completude', porcenta_duas_decimais)

def metrica_comformidade(df_dados_classificados,total_df_sistec):
    #Metrica de comformidade
    comformidade = df_dados_classificados.loc[df_dados_classificados['Dimensão'].isin(['Conformidade/Exatidão', 'Comformidade/Exatidão/Consistência'])]
    comformidade_total_linhas = comformidade.shape[0]
    print('Total de inconsistencia de comformidade', comformidade_total_linhas)
    total_linhas_diferenca = (total_df_sistec - comformidade_total_linhas) 
    print('Diferença de total inconsistencia/ Total linhas dataframe', total_linhas_diferenca)
    porcentagem_qualidade = (total_linhas_diferenca / total_df_sistec) * 100
    porcenta_duas_decimais  = "{:.3f}".format(porcentagem_qualidade)
    print(porcenta_duas_decimais)
    salvar_dados_metrica('Comformidade', porcenta_duas_decimais)

def metrica_consistencia(df_dados_classificados,total_df_sistec):
    #Metrica de consistência
    consistencia = df_dados_classificados.loc[df_dados_classificados['Dimensão'].isin(['Consistência', 'Completude/Consistência','Comformidade/Exatidão/Consistência'])]
    consistencia_total_linhas = consistencia.shape[0]
    print('Total de inconsistencia de consistência', consistencia_total_linhas)
    total_linhas_diferenca = (total_df_sistec - consistencia_total_linhas) 
    print('Diferença de total inconsistencia/ Total linhas dataframe', total_linhas_diferenca)
    porcentagem_qualidade = (total_linhas_diferenca / total_df_sistec) * 100
    porcenta_duas_decimais  = "{:.3f}".format(porcentagem_qualidade)
    print(porcenta_duas_decimais)
    salvar_dados_metrica('Consistência', porcenta_duas_decimais)

def salvar_dados_metrica(nome_dimensao, porcentagem_qualidade):
    lista_data_frame = {'Dimensão': [nome_dimensao],'Calculo da metrica': [porcentagem_qualidade]}
    df_metrica = pd.DataFrame(lista_data_frame)
    print(df_metrica)
    df_metrica.to_csv('inconsistencias/dados_metrica_calculado.csv', mode='a', header=not os.path.exists('inconsistencias/dados_metrica_calculado.csv') , index=False)
