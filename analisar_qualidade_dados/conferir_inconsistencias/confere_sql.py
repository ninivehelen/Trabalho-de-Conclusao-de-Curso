import pandas as pd
import pandasql

# ----------------------------------------------------------------------
# Leitura dos arquivos CSV
# ----------------------------------------------------------------------

# Arquivo principal do Sistec (dados originais)
df_sistec = pd.read_csv('sistec_ifb.csv', sep=';', encoding='utf-8')

# Arquivo com as inconsistências geradas após a validação inicial
df = pd.read_csv('analisar_qualidade_dados/inconsistencias/base1_inconsistencias_unido.csv', sep=',', encoding='utf-8')

# Arquivo com inconsistências já classificadas por dimensão da qualidade
df_classificado = pd.read_csv('analisar_qualidade_dados/inconsistencias/base1_inconsistencias_unido_classificado.csv', sep=',', encoding='utf-8')

# ----------------------------------------------------------------------
# Exibição de informações gerais sobre os datasets
# ----------------------------------------------------------------------
print("Total de linhas data frame Sistec")
total_linhas_df = df_sistec.shape[0]
print(total_linhas_df)

print("Total de linhas data frame inconsistencias")
total_linhas_inconsistencias = df_classificado.shape[0]
print(total_linhas_inconsistencias)

print("Total de linhas data frame classificado")
total_linhas_classificado = df_classificado.shape[0]
print(total_linhas_classificado)

# ----------------------------------------------------------------------
# Consultas SQL com pandasql para conferência das inconsistências
# ----------------------------------------------------------------------

# Consulta 1:
# Verifica quais códigos universais possuem inconsistência
# na dimensão "Completude"
query = """
SELECT codigo_universal_linha, Dimensão
FROM df_classificado
WHERE Dimensão = 'Completude'
GROUP BY codigo_universal_linha
"""
print(pandasql.sqldf(query, locals()))

# Consulta 2:
# Verifica quais códigos universais possuem inconsistência
# do tipo "vazio" no dataset de inconsistências
query_ = """
SELECT codigo_universal_linha, tipo_inconsistencia
FROM df
WHERE tipo_inconsistencia = 'vazio'
GROUP BY codigo_universal_linha
"""
print(pandasql.sqldf(query_, locals()))

# ----------------------------------------------------------------------
# Exemplo adicional de consulta (comentado):
# Permite verificar no dataset original se um aluno específico
# (pelo código universal) realmente existe e se bate com a inconsistência.
# ----------------------------------------------------------------------
# query_ = """
# SELECT 
# * FROM df_sistec 
# WHERE Aluno = '40348'
# GROUP BY Aluno
# """
# print(pandasql.sqldf(query_, locals()))
