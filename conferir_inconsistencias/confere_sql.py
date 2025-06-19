import pandas as pd
import pandasql

# Arquivo sistec 
df_sistec = pd.read_csv('sistec_ifb.csv', sep=';',encoding='utf-8')

# Arquivo com as inconsistencias geradas após a validação
df = pd.read_csv('inconsistencias/base1_inconsistencias_unido.csv')

# Arquivo com as inconsistencias geradas com as dimensões afetadas classificadas 
df_classificado = pd.read_csv('inconsistencias/base1_inconsistencias_unido_classificado.csv')

print("Total de linhas data frame Sistec")
total_linhas_df = df_sistec.shape[0]
print(total_linhas_df)

print("Total de linhas data frame inconsistencias")
total_linhas_inconsistencias = df_classificado.shape[0]
print(total_linhas_inconsistencias)

print("Total de linhas data frame classificado")
total_linhas_classificado = df_classificado.shape[0]
print(total_linhas_classificado)


# Código para conferir as inconsistências e suas clssificações.

# Conferindo as dimensões afetadas
query = """
SELECT codigo_universal_linha, Dimensão
FROM df_classificado
WHERE Dimensão = 'Completude'
GROUP BY codigo_universal_linha
"""
print(pandasql.sqldf(query, locals()))

# Conferindo a inconsistênncia 
query_ = """
SELECT codigo_universal_linha, tipo_inconsistencia
FROM df
WHERE tipo_inconsistencia = 'vazio'
GROUP BY codigo_universal_linha
"""
print(pandasql.sqldf(query_, locals()))

# Nesse é necessário passar o codigo para verificar se a inconsistencias existe
query_ = """
SELECT 
FROM df
WHERE Aluno = '10'
GROUP BY Aluno
"""
print(pandasql.sqldf(query_, locals()))

# query_ = """
# SELECT codigo_universal_linha, tipo_inconsistencia
# FROM df
# WHERE codigo_universal_linha IN ('40348_2467355', '63019_2709325')
# GROUP BY codigo_universal_linha
# """
# print(pandasql.sqldf(query_, locals()))
