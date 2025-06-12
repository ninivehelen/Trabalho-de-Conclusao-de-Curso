import pandas as pd
import pandasql

df_sistec = pd.read_csv('sistec_ifb.csv', sep=';',encoding='utf-8')
df = pd.read_csv('inconsistencias/base1_inconsistencias_unido.csv')
df_classificado = pd.read_csv('inconsistencias/base1_inconsistencias_unido_classificado.csv')
print("Total de linhas data frame Sistec")
total_linhas_df = df_sistec.shape[0]
print(total_linhas_df)
print("Total de linhas data frame classificado")
total_linhas = df_classificado.shape[0]
print(total_linhas)

# Código para conferir as inconsistências e suas clssificações.
query = """
SELECT codigo_universal_linha, Dimensão
FROM df_classificado
WHERE Dimensão = 'Completude'
GROUP BY codigo_universal_linha
"""
print(pandasql.sqldf(query, locals()))

query_ = """
SELECT codigo_universal_linha, tipo_inconsistencia
FROM df
WHERE tipo_inconsistencia = 'vazio'
GROUP BY codigo_universal_linha
"""
print(pandasql.sqldf(query_, locals()))

# query_ = """
# SELECT codigo_universal_linha, tipo_inconsistencia
# FROM df
# WHERE codigo_universal_linha IN ('40348_2467355', '63019_2709325')
# GROUP BY codigo_universal_linha
# """
# print(pandasql.sqldf(query_, locals()))
