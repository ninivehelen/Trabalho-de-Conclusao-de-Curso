import pandas as pd 


# Código criado para realizar tabelas 
df_sistec_inco = pd.read_csv('inconsistencias/sistec_ifb_inconsistencias_unido.csv')
incon_coluna = df_sistec_inco.groupby(['coluna', 'tipo_inconsistencia'])['tipo_inconsistencia'].count()
print(incon_coluna)
print('---------------------------------')
print('---------------------------------')

# Total classificados
df_sistec_inco = pd.read_csv('inconsistencias/sistec_ifb_inconsistencias_unido_classificado.csv')
incon_dime = df_sistec_inco.groupby(['tipo_inconsistencia', 'Dimensão'])['Dimensão'].count()
print(incon_dime)

# Total de inconsisências 
df_sistec_duplicados = pd.read_csv('inconsistencias/sistec_duplicados.csv')
total_duplicados = df_sistec_duplicados.shape[0]
print("total duplicados", total_duplicados)


