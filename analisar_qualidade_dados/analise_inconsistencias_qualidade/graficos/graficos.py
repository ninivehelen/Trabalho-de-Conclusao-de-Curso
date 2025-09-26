import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

# Carregar csv contendo as metricas das dimensões calculadas
df_inconsistencias = pd.read_csv('analisar_qualidade_dados/inconsistencias/sistec_ifb_metrica_calculado.csv')

def gerar_grafico_qualidade():

    """
    Gera um gráfico de barras mostrando a porcentagem de qualidade dos dados 
    por dimensão e salva o arquivo em PNG.

    Parâmetros:
        df (pd.DataFrame): DataFrame contendo as colunas 
                           'Dimensão' e 'Porcentagem da qualidade'.
        caminho_saida (str): Caminho do arquivo PNG para salvar o gráfico.

    Retorna:
        O gráfico é exibido na tela e salvo no caminho especificado.
    """
    print("Procesando gráfico")
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
    sns.set_style("darkgrid") 
    paleta_verde = sns.color_palette(palette='RdYlGn', n_colors=len(categorias_qualidade))

    plt.figure(figsize=(10, 6))
    eixo = sns.barplot(data=df_inconsistencias, x='Dimensão', y='Porcentagem da qualidade', hue='Faixa de Qualidade', palette=paleta_verde, dodge=False)

    eixo.set_xlabel('Dimensões da qualidade dos dados analisadas', fontsize=12,labelpad=15)  # Remover titulo eixo X 

    eixo.set_ylabel('Métrica (%)', fontsize=12)  # Remover titulo eixo Y

    eixo.tick_params(labelsize=13)

    for barra in eixo.containers:
        eixo.bar_label(barra, labels=[f'{p.get_height():.2f}' for p in barra], fontsize=14)

    plt.legend(
        bbox_to_anchor=(1.02, 1),
        loc='upper left',
        borderaxespad=0,
        title='Faixa de Qualidade'
    )

    plt.tight_layout()
    plt.savefig('analisar_qualidade_dados/analise_inconsistencias_qualidade/graficos/grafico_metri.png')
    plt.show()

if __name__ == "__main__":
    """
    Ponto de entrada do script para gerar o gráfico de qualidade dos dados.

    Passos executados:
        1. Chama a função 'gerar_grafico_qualidade' que:
           - Lê o arquivo de métricas calculadas.
           - Classifica os dados em faixas de qualidade.
           - Cria um gráfico de barras mostrando a qualidade por dimensão.
           - Salva o gráfico em PNG e exibe na tela.
    """
    gerar_grafico_qualidade()

