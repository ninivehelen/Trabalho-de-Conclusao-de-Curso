
# Análise de Qualidade de Dados – Sistec IFB

Este projeto faz parte do meu Trabalho de Conclusão de Curso (TCC) para o curso de Ciência da Computação no Instituto Federal de Brasília (IFB).  
Ele realiza a análise de qualidade de dados de registros do Sistec IFB, identificando inconsistências nos dados, classificando as dimensões afetadas (Completude, Exatidão, Conformidade, Consistência e Unicidade), calculando métricas de qualidade e gerando gráficos com os resultados.

---

## Informações importantes sobre o trabalho
O projeto, realizado por uma equipe do IFB em parceria com o Sistec e autorizado pelo TED 10718/2021, teve como foco a definição de métodos e procedimentos para higienização de dados (identificação, correção ou remoção de dados incorretos). Utilizando a base do Sistec, que contém registros da educação profissional e tecnológica do Brasil, a equipe documentou regras de negócio, criou funções de validação e desenvolveu um painel para visualização das informações e identificação de inconsistências.

Os códigos: 
` valida_dados_principal.py`
` veri_dupli_uni_arqu.py `

que fazem a verificação se os dados seguem a regra de negócio são códigos reaproveitados e ajustados para o contexto dos dados do Sistec IFB.

Este trabalho de conclusão de curso, inspirado nesse projeto, busca analisar qualitativa e quantitativamente as inconsistências, avaliar a qualidade dos dados e propor sugestões para minimizar novos problemas. A análise será feita com base em metodologias de qualidade de dados, utilizando registros do IFB disponíveis no portal de dados abertos.

---

## Objetivo Geral

Realizar uma análise quantitativa e qualitativa da qualidade dos dados do Sistec provenientes do IFB, identificando inconsistências e propondo estratégias de melhoria, com o objetivo de aumentar a confiabilidade das informações.

---

## Estrutura do Projeto

```plaintext
trabalho_de_conclusao_de_curso/
    └── analisar_qualidade_dados/
        ├── analise_inconsistencias_qualidade/
        │   └── graficos/
        │       └── graficos.py
        │       └── (após compilar grafico_metri.png aparecerá aqui)
        |   └── analise_inconsistencia.py
        ├── analise_qualidade/
        │   ├── dimensoes_metricas.py
        │   └── main.py
        ├── conferir_inconsistencias/
        │   └── confere_sql.py
        ├── inconsistencias/
        │   └── (após compilar o projeto, arquivos CSV aparecerão aqui)
        
        ├── .gitignore
        ├── sistec_ifb.csv
        ├── valida_dados_lib.py
        ├── valida_dados_principal.py 
        └── veri_dupli_uni_arqu.py
    └── documentacao_dados/
        │   └── documentacao_classificacao_dimensao.pdf 
        │   └── documentacao_regra_negocio_dados_ifb.pdf
    └── README.md
    └── sistec.ifb
```

## Passos para Executar o Projeto

### Passo 1
Na pasta `trabalho_de_conclusao_de_curso`, entre na pasta `analisar_qualidade_dados` execute o arquivo `valida_dados_principal.py`.  
Uma janela será aberta para selecionar o arquivo `sistec_ifb.csv`, que será validado de acordo com as regras de negócio.

Se houver inconsistências:

- Serão gerados arquivos CSV das colunas com irregularidades.
- Automaticamente, o script `veri_dupli_uni_arqu.py` será executado para:
  - Verificar duplicidades nos dados
  - Unir todos os CSVs das inconsistências encontradas

**Resultado do Passo 1:**  
Na pasta `inconsistencias`, você terá:

- Arquivos `sistec_ifb_valida...csv` (um para cada coluna com inconsistência)
- `sistec_ifb_inconsistencias_unido.csv`

---

### Passo 2
Execute o arquivo `main.py` na pasta `analise_qualidade` que está dentro da pasta `analisar_qualidade_dados`

O script irá:

- Classificar as inconsistências de acordo com a dimensão afetada
- Chamar automaticamente as funções do arquivo `dimensoes_metricas.py` para calcular as métricas de cada dimensão e salvar os resultados

---

### Passo 3
Após executar os passos 1 e 2, a pasta `inconsistencias` terá os seguintes arquivos:

- `sistec_duplicados.csv`
- `sistec_ifb_inconsistencias_unido_classificado.csv`
- `sistec_ifb_inconsistencias_unido.cs.csv`
- `sistec_ifb_inconsistencias_unido.csv`
- `sistec_ifb_metrica_calculado.csv`
- Arquivos `sistec_ifb_valida...csv` (dependendo das inconsistências encontradas)

---

### Passo 4
Gerar o gráfico para apresentar os resultados das métricas de forma visual, além de tabelas para visualizar a quantidade de inconsistências por coluna e por dimensão afetada.

Para isso:

- Entre na pasta `analise_inconsistencias_qualidade` que está dentro da pasta `analisar_qualidade_dados` e, dentro da pasta `graficos`, execute o arquivo `graficos.py`.
- Execute também o arquivo `analise_inconsistencia.py` na pasta `analise_inconsistencias_qualidade` para visualizar o quantitativo de inconsistências por coluna e dimensão afetada.

---

## Links Importantes

Para realizar este projeto, foram criadas documentações em PDF sobre os dados, as regras de negócio e uma classificação das dimensões que seriam afetadas caso determinada regra não fosse seguida.
 
Na pasta [documentações dos registros dos dados](https://github.com/ninivehelen/Trabalho-de-Conclusao-de-Curso/tree/31ba36e2c2f3b60d04edc84f0664a2341946640e/documentacao_dados) contém os seguintes arquivos:

- `documentacao_regra_negocio_dados_ifb.pdf`: documento contendo as regras de negócio dos dados  
- `documentacao_classificacao_dimensao.pdf`: documento com a classificação das dimensões afetadas caso o dado não siga a regra

## Sobre os Registros Utilizados
Os dados utilizados são do Sistec IFB, que são dados abertos e podem ser baixados neste [link](https://diretorios.ifb.edu.br/diretorios/1558).  
O portal contém um dicionário detalhado sobre os dados.

---

## 🛠️ Ferramentas Utilizadas  

Para a criação das documentações, foi utilizado o **Figma**:  
<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/figma/figma-original.svg" alt="Figma" width="60" height="60"/>  
</p>


Para a construção das funções que validam as regras de negócio dos dados e calculam as métricas, todo o código foi desenvolvido em **Python**:  
<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/python/python-original.svg" alt="Python" width="60" height="60"/>  
</p>  

Para o carregamento e manipulação dos dados, utilizamos a biblioteca **pandas**:  
<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/devicons/devicon/icons/pandas/pandas-original.svg" alt="Pandas" width="60" height="60"/>  
</p>  

Para a criação de gráficos, foram utilizadas as bibliotecas **Matplotlib** e **Seaborn**:  
<p align="center">
  <img src="https://matplotlib.org/_static/logo2_compressed.svg" alt="Matplotlib" width="60" height="60"/>
  <img src="https://seaborn.pydata.org/_static/logo-wide-lightbg.svg" alt="Seaborn" width="100"/>  
</p>  


 






