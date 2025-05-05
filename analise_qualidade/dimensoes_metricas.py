import pandas as pd 

def abri_arquivo_classificado():
    df_dados_classificados = pd.read_csv('inconsistencias/base1_inconsistencias_unido_classificado.csv', sep=',',encoding='utf-8')
    df_sistec = pd.read_csv('C:/Users/niniv/OneDrive/Área de Trabalho/Trabalho-de-Conclusao-de-Curso/sistec_ifb.csv', sep=',',encoding='utf-8')
    total_linhas_dados = df_dados_classificados.shape[0]
    total_df_sistec = df_sistec.shape[0]
    print('total de linhas arquivo sistec ifb ', total_df_sistec)
    print('total de inconsistencias geral arquivo', total_linhas_dados)
    metrica(df_dados_classificados, total_df_sistec)

def metrica(df_dados_classificados,total_df_sistec):
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


def salvar_dados_metrica(nome_dimensao, porcentagem_qualidade):
    lista_data_frame = {'Dimensão': [nome_dimensao],'Completude dos dados': [porcentagem_qualidade]}
    df_metrica = pd.DataFrame(lista_data_frame)
    print(df_metrica)
    df_metrica.to_csv('inconsistencias/dados_metrica_calculado.csv', sep=',', encoding="utf-8", index=False)
