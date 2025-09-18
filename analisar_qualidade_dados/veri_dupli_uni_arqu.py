import glob
import os 
import pandas as pd 
import numpy as np 
import os.path
diretorio = 'analisar_qualidade_dados/inconsistencias'

def verificar_duplicidade(df_sistec):

    """
    Verifica duplicidades no dataset do Sistec considerando um conjunto
    de colunas específicas que definem registros únicos.

    Parâmetros:
        df_sistec (pd.DataFrame): DataFrame contendo os dados originais do Sistec.

    Efeitos colaterais:
        - Gera um arquivo CSV 'sistec_duplicados.csv' no diretório definido,
          contendo todos os registros duplicados.
    """
    colunas = [
        'Aluno', 'co_ciclo_matricula', 'dt_data_inicio', 'dt_data_fim_previsto',
        'no_curso', 'nome_ciclo', 'situacao_matricula', 'periodo_cadastro_matricula_aluno'
    ]
    # Seleciona os registros duplicados considerando as colunas acima
    duplicados = df_sistec[df_sistec.duplicated(subset=colunas, keep=False)]
    # Ordena os duplicados pelo código do aluno
    duplicados = duplicados.sort_values(by='Aluno')
    # Salva os registros duplicados em CSV
    duplicados.to_csv(f'{diretorio }/sistec_duplicados.csv', sep=',', index=False, encoding='utf-8-sig')

def unir_arquivos():
    
    """
    Une todos os arquivos CSV de inconsistências do Sistec que seguem o padrão
    'sistec_ifb*.csv', substitui valores ausentes por 'vazio' e gera um CSV unificado.

    Resulado:
        - Cria o arquivo 'sistec_ifb_inconsistencias_unido.csv' no diretório definido.
    """
    arquivos_csv = glob.glob(os.path.join(diretorio+ "/sistec_ifb*.csv")) 
    print(arquivos_csv)
    base_dados = pd.DataFrame()
    for arquivo in arquivos_csv:
        df_temp = pd.read_csv(arquivo)
        base_dados  = pd.concat([df_temp,base_dados])
    df_dados_ = base_dados.replace(np.nan, 'vazio')
    df_dados_.to_csv(f'{diretorio }/sistec_ifb_inconsistencias_unido.csv', index=False)
    print("CSV com todas as inconsistências criado")

