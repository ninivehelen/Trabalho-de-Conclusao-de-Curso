import pandas as pd 
import os 
import os
import numpy as np
import os.path
diretorio = 'analisar_qualidade_dados/inconsistencias/'

def abri_arquivo_classificado():

    """
    Lê os arquivos originais do Sistec e o arquivo classificado,
    imprime o total de linhas em cada um e retorna os dados classificados.

    Retorna:
        tuple:
            - df_dados_categorizados (pd.DataFrame): inconsistências classificadas por dimensão.
            - total_df_sistec (int): número total de linhas do arquivo original.
    """

    print('--------------------------------------------------')
    df_sistec = pd.read_csv('sistec_ifb.csv', sep=';',encoding='utf-8')
    df_dados_categorizados = pd.read_csv(diretorio+'sistec_ifb_inconsistencias_unido_classificado.csv', sep=',',encoding='utf-8')
    total_df_sistec = df_sistec.shape[0]
    total_linhas_dados =  df_dados_categorizados.shape[0]
    print('Total de linhas dados Sistec IFB', total_df_sistec)
    print('Total de linhas dados Sistec IFB classificado', total_linhas_dados)
    print('---------------------------')
    return(df_dados_categorizados, total_df_sistec)

def metrica_unicidade(total_df_sistec):

    """
    Calcula a métrica de unicidade: verifica duplicidades no dataset
    e calcula a porcentagem de qualidade dos dados.

    Parâmetros:
        total_df_sistec (int): total de linhas do dataset original.
    """

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

    """
    Calcula a métrica de exatidão: analisa inconsistências de exatidão
    e calcula a porcentagem de qualidade dos dados.

    Parâmetros:
        df_dados_categorizados (pd.DataFrame): inconsistências classificadas por dimensão.
        total_df_sistec (int): total de linhas do dataset original.
    """

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

    """
    Calcula a métrica de completude: verifica registros incompletos
    e calcula a porcentagem de qualidade dos dados.

    Parâmetros:
        df_dados_categorizados (pd.DataFrame): inconsistências classificadas por dimensão.
        total_df_sistec (int): total de linhas do dataset original.
    """

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

    """
    Calcula a métrica de conformidade: analisa inconsistências de conformidade
    e calcula a porcentagem de qualidade dos dados.

    Parâmetros:
        df_dados_categorizados (pd.DataFrame): inconsistências classificadas por dimensão.
        total_df_sistec (int): total de linhas do dataset original.
    """

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

    """
    Calcula a métrica de consistência: verifica inconsistências relacionadas
    à consistência dos dados e calcula a porcentagem de qualidade.

    Parâmetros:
        df_dados_categorizados (pd.DataFrame): inconsistências classificadas por dimensão.
        total_df_sistec (int): total de linhas do dataset original.
    """

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

    """
    Salva os resultados das métricas em um arquivo CSV.

    Parâmetros:
        nome_dimensao (str): Nome da dimensão avaliada.
        porcentagem_qualidade (float|str): Percentual de qualidade calculado.
    """
    
    lista_data_frame = {'Dimensão': [nome_dimensao],'Porcentagem da qualidade': [porcentagem_qualidade]}
    df_metrica = pd.DataFrame(lista_data_frame)
    print(df_metrica)
    caminho = 'analisar_qualidade_dados/inconsistencias/sistec_ifb_metrica_calculado.csv'
    caminho_arquivo = os.path.exists(caminho)
    df_metrica.to_csv(caminho, mode='a' if caminho_arquivo else 'w', header=not caminho_arquivo,index=False)

