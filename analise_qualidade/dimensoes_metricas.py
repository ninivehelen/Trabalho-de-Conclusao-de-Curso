import pandas as pd 
import os 
import os
import numpy as np
import os.path
diretorio = 'inconsistencias'

def abri_arquivo_classificado():
    print('---------------------------')
    df_sistec = pd.read_csv('sistec_ifb.csv', sep=';',encoding='utf-8')
    df_dados_categorizados = pd.read_csv('inconsistencias/sistec_ifb_inconsistencias_unido_classificado.csv', sep=',',encoding='utf-8')
    total_df_sistec = df_sistec.shape[0]
    total_linhas_dados =  df_dados_categorizados.shape[0]
    print('Total de linhas dados Sistec IFB', total_df_sistec)
    print('Total de linhas dados Sistec IFB classificado', total_linhas_dados)
    print('---------------------------')
    return(df_dados_categorizados, total_df_sistec)

def metrica_unicidade(total_df_sistec):
    #Metrica de unicidade
    print('iniciando a metrica de unicidade')
    if os.path.exists(f'{diretorio}/sistec_duplicados.csv'):
        df_duplicado = pd.read_csv(f'{diretorio}/sistec_duplicados.csv', sep=',',encoding='utf-8')
        duplicado_total_linhas = df_duplicado.shape[0]
        print('Total de linhas duplicadas de unicidade', duplicado_total_linhas)
        total_linhas_diferenca = (total_df_sistec - duplicado_total_linhas) 
        print('Diferença de total de duplicadas/ Total linhas dataframe', total_linhas_diferenca)
        porcentagem_qualidade = (total_linhas_diferenca / total_df_sistec) * 100

        formatado_resu = int(porcentagem_qualidade * 100) / 100
        porcenta_tres_decimais = "{:.2f}".format(formatado_resu)

        print("porcentagem da qualidade dos dados", porcenta_tres_decimais)
        salvar_dados_metrica('Unicidade', porcenta_tres_decimais)
    else:
        print("Arquivo duplicidade não existe, tem duplicidades")

def metrica_exatidao(df_dados_categorizados,total_df_sistec):
    #Metrica de exatidao
    print('iniciando a metrica de exatidão')
    exatidao =  df_dados_categorizados.loc[ df_dados_categorizados['Dimensão'].isin(['Conformidade/Exatidão', 'Conformidade/Exatidão/Consistência'])]
    exatidao_total_linhas = exatidao.shape[0]
    print('Total de inconsistencia de exatidão', exatidao_total_linhas)
    total_linhas_diferenca = (total_df_sistec - exatidao_total_linhas) 
    print('Diferença de total inconsistencia/ Total linhas dataframe', total_linhas_diferenca)
    porcentagem_qualidade = (total_linhas_diferenca / total_df_sistec) * 100
    
    formatado_resu = int(porcentagem_qualidade * 100) / 100
    porcenta_tres_decimais = "{:.2f}".format(formatado_resu)

    print("porcentagem da qualidade dos dados", porcenta_tres_decimais)
    salvar_dados_metrica('Exatidão', porcenta_tres_decimais)
  
def metrica_completude(df_dados_categorizados,total_df_sistec):
    #Metrica de completude
    print('iniciando a metrica de completude')
    completude =  df_dados_categorizados[df_dados_categorizados['Dimensão'] == 'Completude']
    completude_total_linhas = completude.shape[0]
    print('Total de inconsistencia de completude', completude_total_linhas)
    total_linhas_diferenca = (total_df_sistec - completude_total_linhas) 
    print('Diferença de total inconsistencia/ Total linhas dataframe', total_linhas_diferenca)
    porcentagem_qualidade = (total_linhas_diferenca / total_df_sistec) * 100

    formatado_resu = int(porcentagem_qualidade * 100) / 100
    porcenta_tres_decimais = "{:.2f}".format(formatado_resu)

    print("porcentagem da qualidade dos dados", porcenta_tres_decimais)
    salvar_dados_metrica('Completude', porcenta_tres_decimais)

def metrica_conformidade( df_dados_categorizados,total_df_sistec):
    #Metrica de conformidade
    print('iniciando a metrica de conformidade')
    conformidade =  df_dados_categorizados.loc[df_dados_categorizados['Dimensão'].isin(['Conformidade/Exatidão', 'Conformidade/Exatidão/Consistência'])]
    conformidade_total_linhas = conformidade.shape[0]
    print('Total de inconsistencia de conformidade', conformidade_total_linhas)
    total_linhas_diferenca = (total_df_sistec - conformidade_total_linhas) 
    print('Diferença de total inconsistencia/ Total linhas dataframe', total_linhas_diferenca)
    porcentagem_qualidade = (total_linhas_diferenca / total_df_sistec) * 100
    
    formatado_resu = int(porcentagem_qualidade * 100) / 100
    porcenta_tres_decimais = "{:.2f}".format(formatado_resu)

    print("porcentagem da qualidade dos dados", porcenta_tres_decimais)
    salvar_dados_metrica('Conformidade',porcenta_tres_decimais)

def metrica_consistencia( df_dados_categorizados,total_df_sistec):
    #Metrica de consistência
    print('iniciando a metrica de consistencia')
    consistencia = df_dados_categorizados.loc[df_dados_categorizados['Dimensão'].isin(['Consistência', 'Completude/Consistência','Conformidade/Exatidão/Consistência'])]
    consistencia_total_linhas = consistencia.shape[0]
    print('Total de inconsistencia de consistência', consistencia_total_linhas)
    total_linhas_diferenca = (total_df_sistec - consistencia_total_linhas) 
    print('Diferença de total inconsistencia/ Total linhas dataframe', total_linhas_diferenca)
    porcentagem_qualidade = (total_linhas_diferenca / total_df_sistec) * 100
    
    formatado_resu = int(porcentagem_qualidade * 100) / 100
    porcenta_tres_decimais = "{:.2f}".format(formatado_resu)

    print("porcentagem da qualidade dos dados", porcenta_tres_decimais)
    salvar_dados_metrica('Consistência', porcenta_tres_decimais)

def salvar_dados_metrica(nome_dimensao, porcentagem_qualidade):
    lista_data_frame = {'Dimensão': [nome_dimensao],'Porcentagem da qualidade': [porcentagem_qualidade]}
    df_metrica = pd.DataFrame(lista_data_frame)
    print(df_metrica)
    caminho = 'inconsistencias/sistec_ifb_metrica_calculado.csv'
    caminho_arquivo = os.path.exists(caminho)
    df_metrica.to_csv(caminho, mode='a' if caminho_arquivo else 'w', header=not caminho_arquivo,index=False)

