import pandas as pd 
from dimensoes_metricas import *

nome = '/base1_inconsistencias_unido.csv'
diretorio = 'analisar_qualidade_dados/inconsistencias/'

# O dicionário chamado dimensao_dicionario ajuda a adicionar qual dimensão foi afetada de acordo com a descrição da inconsistência.
def dimensao_busca(nome_inconsistencia):

    """
    Retorna a dimensão da qualidade dos dados associada a um tipo de inconsistência específico.

    Parâmetros:
        nome_inconsistencia (str): Nome da inconsistência identificada.

    Retorna:
        str: Nome da dimensão da qualidade correspondente.
    """

    dimensao_dicionario =  {
    'vazio'  :  'Completude',
    'tipo_não_valido'  :  'Conformidade/Exatidão',
    'caracter_especial'  :  'Conformidade/Exatidão',
    'estado_uma_sigla_não_valida': 'Conformidade/Exatidão',
    'valor_diferente_F_M': 'Conformidade/Exatidão',
    'cnpj_invalido' :'Conformidade/Exatidão',
    'não_conforme_a_regra': 'Consistência',
    'diferente_hifen_e_não_conforme_a_regra': 'Consistência',
    'vazio_não_conforme_a_regra': 'Completude/Consistência',
    'diferente_dos_eixos_e_não_comforme_a_regra': 'Conformidade/Exatidão/Consistência',
    'booleano_invalido': 'Conformidade/Exatidão',
    'booleano_invalido_lista_cota':'Conformidade/Exatidão',
    'data_invalida':'Conformidade/Exatidão',
    'vazio_esperado_não_conforme_a_regra':'Consistência',
    'dt_cadastro_ciclo_inferior_não_conforme_a_regra':'Consistência',
    'dt_deferimento_ue_superior_não_conforme_a_regra':'Consistência',
    'dt_cadastro_aluno_sistema_inferior_não_conforme_a_regra':'Consistência',
    'ano_invalido':'Conformidade/Exatidão',
    'situacao_com_cpf_cpf_vazio': 'Completude/Consistência',
    'cpf_invalido':'Conformidade/Exatidão',
    'situacao_sem_cpf_cpf_preenchido':'Consistência',
    'dt_data_fim_previsto_inferior_data_inicio': 'Consistência',
    'dt_data_fim_previsto_inferior_ou_igual_data_inicio': 'Consistência',
    'diferente_1_ou_3_situacao_não_concluida':'Consistência',
    'data_geracao_existe_codigo_diploma_não':'Conformidade/Exatidão/Consistência',
    'data_geracao_não_existe_codigo_diploma_sim':'Conformidade/Exatidão/Consistência',
    'ano_maior_que_o_ano_atual_não_conforme_a_regra':'Consistência',
    'dt_geracao_existe_dt_validacao_não_existe_não_conforme_a_regra':'Completude/Consistência',
    }
    return dimensao_dicionario[nome_inconsistencia]

def classificar_dimensoes(df_dados):

    """
    Classifica as inconsistências do DataFrame de acordo com a dimensão afetada.

    Parâmetros:
        df_dados (pd.DataFrame): DataFrame contendo as inconsistências e suas descrições.

    Efeitos colaterais:
        - Adiciona uma nova coluna 'Dimensão' ao DataFrame.
        - Chama a função salvar_classificacao() para gravar o arquivo classificado.
    """
    
    print("classificando inconsistências para adicionar a dimensão afetada.")
    dimensao = []
    # Percorre cada linha do DataFrame e identifica a dimensão com base na inconsistência
    for index, linha in df_dados.iterrows():
        nome_inconsistencia = linha['tipo_inconsistencia']
        dimensao.append(dimensao_busca(nome_inconsistencia))
    df_dados['Dimensão'] = dimensao
    salvar_classificacao(df_dados)

def salvar_classificacao(df_dados):

    """
    Salva o DataFrame classificado com a dimensão de inconsistência em um arquivo CSV.

    Parâmetros:
        df_dados (pd.DataFrame): DataFrame contendo as inconsistências já classificadas.

    Saída:
        - Arquivo CSV gravado no diretório configurado.
    """

    df_dados.to_csv(diretorio + '/sistec_ifb_inconsistencias_unido_classificado.csv', index=False)
    print('arquivo salvo com a dimensão classificada')

if __name__ == "__main__":
    
    """
    Pipeline principal de execução do script de análise de inconsistências
    e métricas de qualidade dos dados do Sistec.

    Fluxo de execução:
        1. Carrega o arquivo unificado de inconsistências.
        2. Classifica cada inconsistência de acordo com a dimensão afetada.
        3. Abre o arquivo classificado e obtém o total de linhas do dataset original.
        4. Calcula as métricas de qualidade dos dados:
           - Unicidade
           - Exatidão
           - Completude
           - Conformidade
           - Consistência
        5. Imprime os resultados das métricas no console e salva em CSV.
    """
     # Carrega o arquivo original de inconsistências
    df_dados = pd.read_csv('analisar_qualidade_dados/inconsistencias/sistec_ifb_inconsistencias_unido.csv')
    classificar_dimensoes(df_dados)
     # Classifica as inconsistências de acordo com a dimensão afetada
    df_dados_classificados, total_df_sistec = abri_arquivo_classificado()
    # Calcula e imprime as métricas de qualidade dos dados
    metrica_unicidade(total_df_sistec)
    metrica_exatidao(df_dados_classificados,total_df_sistec)
    metrica_completude(df_dados_classificados, total_df_sistec)
    metrica_conformidade(df_dados_classificados,total_df_sistec)
    metrica_consistencia(df_dados_classificados,total_df_sistec)
   
   
    




