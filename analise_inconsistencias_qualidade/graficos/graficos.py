import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

df_inconsistencias = pd.read_csv('inconsistencias/sistec_ifb_metrica_calculado.csv')

valores_limite = [0, 20, 40, 60, 80, 100]
categorias_qualidade = [
    '0 ≤ DQ < 20 (muito fraco)',
    '20 ≤ DQ < 40 (fraco)',
    '40 ≤ DQ < 60 (médio)',
    '60 ≤ DQ < 80 (bom)',
    '80 ≤ DQ ≤ 100 (excelente)'
]

df_inconsistencias['Faixa de Qualidade'] = pd.cut(df_inconsistencias['Porcentagem da qualidade'],bins=valores_limite,labels=categorias_qualidade, include_lowest=True, right=False
)

paleta_verde = sns.light_palette("seagreen", n_colors=len(categorias_qualidade))

plt.figure(figsize=(10, 6))
eixo = sns.barplot(data=df_inconsistencias, x='Dimensão', y='Porcentagem da qualidade', hue='Faixa de Qualidade', palette=paleta_verde, dodge=False
)

eixo.set_ylabel("Resultado da métrica em porcentagem")

for barra in eixo.containers:
    eixo.bar_label(barra, fontsize=10)

plt.legend(
    bbox_to_anchor=(1.02, 1),
    loc='upper left',
    borderaxespad=0,
    title='Faixa de Qualidade'
)

plt.tight_layout()
plt.savefig('analise_inconsistencias_qualidade/graficos/grafico_dimensao.png')
plt.show()


