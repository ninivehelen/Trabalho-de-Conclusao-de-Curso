import pandas as pd 

def criar_tabelas():

    """
    Lê arquivos CSV com inconsistências do Sistec,
    realiza agrupamentos e contagens,
    e imprime estatísticas de resumo.

    Fluxo do processo:
        1. Carrega inconsistências classificadas e gera agrupamento por coluna, tipo e dimensão.
        2. Conta o total de inconsistências duplicadas.
        3. Conta o total geral de inconsistências (incluindo duplicadas).
    """
    
    # Carregar inconsistências classificadas
    df_sistec_inco = pd.read_csv('analisar_qualidade_dados/inconsistencias/sistec_ifb_inconsistencias_unido_classificado.csv')

    # Agrupar inconsistências por coluna, tipo e dimensão
    incon_dime = df_sistec_inco.groupby(['coluna','tipo_inconsistencia', 'Dimensão'])['Dimensão'].count()
    print(incon_dime)

    # Total de inconsisências dimensão unicidade para saber quantidade de duplicados
    df_sistec_duplicados = pd.read_csv('analisar_qualidade_dados/inconsistencias/sistec_duplicados.csv')
    total_duplicados = df_sistec_duplicados.shape[0]
    print("total duplicados", total_duplicados)

    # Total de inconsisências
    df_sistec_d = pd.read_csv('analisar_qualidade_dados/inconsistencias/sistec_ifb_inconsistencias_unido.csv')
    total_d = df_sistec_d.shape[0]
    print("total duplicados", (total_d + total_duplicados))

if __name__ == "__main__":
    """
    Ponto de entrada do script para gerar tabelas resumidas de inconsistências do Sistec.

    Passos executados:
        1. Chama a função 'criar_tabelas', que:
           - Lê o arquivo de inconsistências classificadas.
           - Agrupa os dados por coluna, tipo de inconsistência e dimensão.
           - Imprime o resumo das contagens por grupo.
           - Lê o arquivo de duplicados e imprime o total de registros duplicados.
           - Lê o arquivo completo de inconsistências e imprime o total geral (incluindo duplicados).
    """
    criar_tabelas()
