import pandas as pd 

diretorio = 'inconsistencias'
def abri_arquivo_classificado(df_dados_classificados):
    total_linhas_dados = df_dados_classificados.shape[0]
    print('total de inconsistencias arquivo', total_linhas_dados)
    metrica_completude(df_dados_classificados,total_linhas_dados)

def metrica_completude(df_dados_classificados, total_linhas_dados):
    completude = df_dados_classificados[df_dados_classificados['Dimensão'] == 'Completude']
    completude_total_linhas = completude.shape[0]
    print('total de inconsistencias completude', completude_total_linhas)
    dados_qualidade = total_linhas_dados - completude_total_linhas 
    porcentagem_qualidade = dados_qualidade / total_linhas_dados
    print(porcentagem_qualidade)
     