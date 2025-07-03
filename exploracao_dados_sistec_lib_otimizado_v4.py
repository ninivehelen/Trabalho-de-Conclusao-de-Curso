
import pandas as pd
import numpy as np
import math
import time
import re
import os 
# Biblioteca para controlar a visualização dos processo
# import ray
from datetime import datetime
# Biblioteca para controlar a visualização dos processo
from tqdm import tqdm
from itertools import cycle

diretorio_principal = os.getcwd()
print(diretorio_principal)

DIR_INCONSISTENCIAS = f'{diretorio_principal}/inconsistencias'
# Condição para verificar se o diretório já existe
if os.path.isdir(DIR_INCONSISTENCIAS):
    print("Diretório já existe: ", DIR_INCONSISTENCIAS)
else:
    os.makedirs(DIR_INCONSISTENCIAS)
    os.chdir(DIR_INCONSISTENCIAS)
    print("Diretório criado: ", DIR_INCONSISTENCIAS)

print("\n>> Arquivo [exploracao_dados_sistec_lib_otimizado_v4] adicionado com sucesso.\n")

# Variável global para receber os valores de tempo de processamento das funções
global_tempo_gasto_funcoes = {}

'''
    INICIO: ↓  Funções para limpeza de cada coluna  ↓
'''

"""
    A ideia básico do padrão de analise usa um ponteiro para varrer as colunas
    e outro para varrer os registros. Assim, podemos passar para as funções
    a(s) coluna(s) contendo todos os registros para processar.
    Ex.:

                    posicaoColuna
                            ↓
    -------------------------------------------------------------------------
    co_unidade_ensino | co_simec | unidade_ensino | dt_deferimento_ue | ... |
    -------------------------------------------------------------------------
    23                | 9874     | SENAC          | 21/03/2009        |     |
    131               | 65467    | SENAI          | 22/01/2009        |     |
    45                | 321      | IFB            | 10/05/2011        |     | ← posicaoLinha
    78                | 11       | SENAC          | 03/03/2010        |     |
    77                | 7412     | SEST           | 15/04/2012        |     |
    ...
    -------------------------------------------------------------------------

    Portanto, passaremos para a função a coluna inteira dos dados a analisar e
    a coluna inteira dos códigos únicos da linha, e a função percorrerá
    todas as linhas(registros) analisando e fazendo os processos necessários.

    Portanto, as funções específicas para limpeza podem receber, pelo menos, dois
    parâmetros importantes:
        df_colunas: deve conter a coluna de identificação e as colunas a processar
        base_info: deve conter um dicionário no formato: {'id': base, 'nome': nome_base}


    OBS.: A documentação das funções devem seguir o padrão do Pandas Docstring Guide:
    > https://pandas.pydata.org/docs/development/contributing_docstring.html

"""

def buscar_dados_repetidos(df_base_dados):
    """
        Função para buscar dados repetidos na base toda

        Parameters
        ----------
        df_base_dados : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        DataFrame
            Retorna as linhas duplicadas encontradas exceto a primeira ocorrência.

        Examples
        --------
        >>> buscar_dados_repetidos( df_base_dados, base_info )
    """

    print("\nIniciando os processos da função: [buscar_dados_repetidos]");
    start = time.perf_counter()

    # retorna da segunda duplicidade encontrada em diante.
    df_temp = df_base_dados[df_base_dados.duplicated()]
 
    end = time.perf_counter()

    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['buscar_dados_repetidos'] = tempo_gasto
    print(f"Tempo total da Função (buscar_dados_repetidos): {tempo_gasto} seg.")

    return df_temp


def valida_inteiro_nao_nulo(df_colunas, base_info):
    """
        Função para verificar as colunas do tipo inteiro que NÃO podem ficar
        vazias e devem ser numéricas.

        Colunas validadas:

            1.  co_unidade_ensino
            8.  co_ciclo_matricula
            12. co_curso
            15. co_tipo_curso
            17. co_tipo_nivel
            22. carga_horaria
            57. co_aluno
            58. co_matricula
            65. co_portifolio

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_inteiro_nao_nulo(df_colunas, base_info)
    """

    print("Iniciando os processos da função: [valida_inteiro_nao_nulo]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]

                if (type(valor) == type(np.nan)):
                    novaLinha = [[codUni, coluna, 'NaN', categoria, cod_coluna]]
                    resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
                else:
                    if not isinstance(valor, int) and not valor.isnumeric():
                        novaLinha = [[codUni, coluna, 'tipo_não_valido', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

            if len(resultado.index) != 0:
                nome_funcao = 'valida_inteiro_não_nulo_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_inteiro_nao_nulo'] = tempo_gasto
    print(f"Tempo total da Função (valida_inteiro_nao_nulo): {tempo_gasto} seg.")


def valida_inteiro(df_colunas, base_info):
    """
        Função para verificar as colunas do tipo inteiro que podem ficar
        vazias e devem ser numéricas.

        Colunas validadas:

            38. cod_municipio
            43. vagas_ofertadas
            44. total_inscritos
            61. co_inep

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_inteiro( df_colunas, base_info )
    """
    print("Iniciando os processos da função: [valida_inteiro]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]

                # Como essa função pode aceitar nulo, se o type valor for diferente do type do valor nulo, então
                # a função passa a validar se é um inteiro.
                if (type(valor) != type(np.nan)):
                    if not isinstance(valor, int) and not valor.isnumeric():
                        novaLinha = [[codUni, coluna, 'tipo_não_valido', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

            if len(resultado.index) != 0:
                nome_funcao = 'valida_inteiro_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_inteiro'] = tempo_gasto
    print(f"Tempo total da Função (valida_inteiro): {tempo_gasto} seg.")


def valida_ds_eixo_tecnologico(df_colunas, base_info):
    """
        Função que deve validar a coluna ds_eixo_tecnologico de acordo com a
        coluna co_tipo_curso, a qual deve ser técnico e tecnólogo (3,4).
        E a coluna ds_eixo_tecnologico deve estar compreendida dentro da lista
        de eixos tecnológicos.

        Colunas validadas:

            11. ds_eixo_tecnologico

        Colunas auxiliares:

            15. co_tipo_curso

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_ds_eixo_tecnologico( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_ds_eixo_tecnologico]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    lista_ds_eixo_tecnologico = 'AMBIENTE E SAÚDE DESENVOLVIMENTO EDUCACIONAL E SOCIAL CONTROLE E PROCESSOS INDUSTRIAIS GESTÃO E NEGÓCIOS TURISMO, HOSPITALIDADE E LAZER INFORMAÇÃO E COMUNICAÇÃO INFRAESTRUTURA MILITAR PRODUÇÃO ALIMENTÍCIA PRODUÇÃO CULTURAL E DESIGN PRODUÇÃO INDUSTRIAL RECURSOS NATURAIS SEGURANÇA'
    lista_co_tipo_curso = [3, 4]
    categoria = buscar_categoria('ds_eixo_tecnologico')

    coluna = "ds_eixo_tecnologico"
    print(coluna)
    cod_coluna = buscar_cod_coluna(coluna)

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])
        co_tipo_curso = linha['co_tipo_curso']
        conteudo = str(linha[coluna])

        # Verifica se o código é técnico ou tecnólogo, se for deve existir um eixo tecnologico
        # 3, 4 : Tecnico 3 e 4
        if co_tipo_curso in lista_co_tipo_curso:
            if (type(conteudo) == type(np.nan)):
                novaLinha = [[codUni, coluna, 'vazio_não_comforme_a_regra', categoria, cod_coluna]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)
            else:
              if not conteudo.upper() in lista_ds_eixo_tecnologico:
                novaLinha = [[codUni, coluna, 'diferente_dos_eixos_e_não_comforme_a_regra', categoria, cod_coluna]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

    if len(resultado.index) != 0:
        nome_funcao = 'valida_ds_eixo_tecnologico_' + coluna
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_ds_eixo_tecnologico'] = tempo_gasto
    print(f"Tempo total da Função (valida_ds_eixo_tecnologico): {tempo_gasto} seg.")


def valida_texto_nao_nulo(df_colunas, base_info):
    """
        Esta função deve validar os campos do tipo texto que não podem ser
        vazios.

        Colunas validadas:

            2. unidade_ensino
            4. dependencia_admin
            5. sistema_ensino
            6. municipio
            13. no_curso
            16. tipo_curso
            18. ds_tipo_nivel
            20. nome_ciclo
            24. nome_aluno
            28. modalidade_pagto
            29. situacao_matricula
            39. modalidade_ensino
            49. status_do_ciclo_de_matricula
            50. nome_completo_da_instituicao
            51. sigla_da_instituicao

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_texto_nao_nulo( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_texto_nao_nulo]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':

            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)
            print("\nColuna: " + str(coluna) + " | Categoria: " + str(categoria) + " | cód. coluna: " + str(cod_coluna))

            for linha in tqdm(df_dict):

                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]

                if (type(valor) == type(np.nan)):
                    novaLinha = [[codUni, coluna, 'NaN', categoria, cod_coluna]]
                    resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
                else:
                    if type(valor) != str:
                        novaLinha = [[codUni, coluna, 'tipo_não_valido', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
                    else:
                        if not valor[:1].isalpha():
                            novaLinha = [[codUni, coluna, 'caracter_especial', categoria, cod_coluna]]
                            resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

            if len(resultado.index) != 0:
                nome_funcao = 'valida_texto_não_nulo_' + str(coluna)
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)


    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_texto_nao_nulo'] = tempo_gasto
    print(f"Tempo total da Função (valida_texto_nao_nulo): {tempo_gasto} seg.")


def valida_estado(df_colunas, base_info):
    """
        Esta função verifica se o conteúdo da coluna é um estado válido ou não.
        Deve conter 2 caracteres alfanuméricos (sigla dos Estados) e não nulo

        Colunas validadas:

            07. estado

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_estado( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_estado]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    # Lista da sigla dos estados brasileiros
    sigla_estado = 'RO AC AM RR PA AP TO MA PI CE RN PB PE AL SE BA MG ES RJ SP PR SC RS MS MT GO DF'

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]

                if (type(valor) == type(np.nan)):
                    novaLinha = [[codUni, coluna, 'NaN', categoria, cod_coluna]]
                    resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
                else:
                    """
                        Upper para que caso o valor seja Df ou df.
                        Para que o dado padronize com a lista sigla_estado,
                        pois assim não é uma incosistência
                    """
                    if not valor.upper() in sigla_estado:
                        novaLinha = [[codUni, coluna, 'estado_uma_sigla_não_valida', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

            if len(resultado.index) != 0:
                nome_funcao = 'valida_estado_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_estado'] = tempo_gasto
    print(f"Tempo total da Função (valida_estado): {tempo_gasto} seg.")


def valida_sexo_aluno(df_colunas, base_info):
    """
        Função que valida a coluna sexo_aluno a fim de gerar inconsistências
        para valores nulos, e aqueles diferentes de 'M' ou 'F'.

        Colunas validadas:

            25. sexo_aluno

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_sexo_auno( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_sexo_aluno]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )

    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]

                if (type(valor) == type(np.nan)):
                    novaLinha = [[codUni, coluna, 'NaN', categoria, cod_coluna]]
                    resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
                else:
                    # A função upper para padronizar, pois caso seja m ou f. Não gerar incosistência
                    if valor.upper() != 'M' and valor.upper() != 'F':
                        novaLinha = [[codUni, coluna, 'valor_diferente_F_M', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

            if len(resultado.index) != 0:
                nome_funcao = 'valida_sexo_aluno_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_sexo_aluno'] = tempo_gasto
    print(f"Tempo total da Função (valida_sexo_aluno): {tempo_gasto} seg.")


def valida_texto(df_colunas, base_info):
    """
        Esta função deve analisar as colunas que deveriam ser do tipo texto.
        Deste modo, gera inconsistências para valores diferentes de string e
        se possuieram caracteres especiais.

        Colunas validadas:

            23. programa_associado
            56. nacionalidade

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_texto( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_texto]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]

                if (type(valor) != type(np.nan)):
                    if type(valor) != str:
                        novaLinha = [[codUni, coluna, 'tipo_não_valido', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
                    else:
                        if not valor[0].isalpha():
                            novaLinha = [[codUni, coluna, 'caracter_especial', categoria, cod_coluna]]
                            resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

            if len(resultado.index) != 0:
                nome_funcao = 'valida_texto_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_texto'] = tempo_gasto
    print(f"Tempo total da Função (valida_texto): {tempo_gasto} seg.")


def valida_subdependencia(df_colunas, base_info):
    """
        Esta função valida a coluna subdependencia.
        Caso o valor da coluna "dependencia_admin" seja "Artigo 240 - Sistema S"
        ou "Militar" deve estar preenchida a coluna subdependencia, caso o valor
        da coluna "dependencia_admin" seja "Pública" ou "Privada" a coluna subdependencia
        deve estar vazia, também deve-se verificar se a subdependencia possui
        um valor numérico.

        Colunas validadas:

            36. subdependencia

        Colunas auxiliares:

            4. dependencia_admin

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_subdependencia( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_subdependencia]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    coluna = 'subdependencia'
    print(coluna)
    categoria = buscar_categoria(coluna)
    cod_coluna = buscar_cod_coluna(coluna)

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])
        dependencia_admin = linha['dependencia_admin']
        subdependencia = linha['subdependencia']

        if (dependencia_admin.lower()) == 'artigo 240 - sistema s' or (dependencia_admin.lower()) == 'militar' :
           if ((type(subdependencia) == type(np.nan)) or (subdependencia == "-")):
               novaLinha = [[codUni, coluna, 'vazio_não_conforme_a_regra', categoria, cod_coluna]]
               resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

        else:
           if (dependencia_admin.lower()) == 'pública' or (dependencia_admin.lower()) == 'privada' :
               if ((type(subdependencia) != type(np.nan)) or (subdependencia != "-")):
                   novaLinha = [[codUni, coluna, 'vazio_esperado_não_conforme_a_regra', categoria, cod_coluna]]
                   resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

    if len(resultado.index) != 0:
        nome_funcao = 'valida_subdependencia_' + coluna
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna)


    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_subdependencia'] = tempo_gasto
    print(f"Tempo total da Função (valida_subdependencia): {tempo_gasto} seg.")


def valida_email(df_colunas, base_info):
    """
        # DEPRECATED: Função não mais usada.

        Função que fará a validação dos campos do tipo texto da base dados que
        são referentes às colunas da base de dados fornecida:

        Colunas validadas:

            61.email_aluno

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter as colunas 'codigo_universal_linha' e as
            colunas a serem processadas por essa função()

        Returns
        -------
        DataFrame
            Deve retornar um dataframe com os dados da linha com problema sendo:
            codigo_universal_linha, coluna, tipo_inconsistencia

        Examples
        --------
        >>> valida_email( df_colunas, base_info )
    """
    print("Iniciando os processos da função: [valida_email]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]
                valor = str(valor)

                # Passando o email para a função que verifica se o e_mail é valido
                email_aluno = verifica_email(valor)

                # Se a função retorna False. o Email é invalido
                if (email_aluno == False):
                    novaLinha = [[codUni, 'email_aluno', 'email_invalido', categoria, cod_coluna]]
                    resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

            if len(resultado.index) != 0:
                nome_funcao = 'valida_email_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_email'] = tempo_gasto
    print(f"Tempo total da Função (valida_email): {tempo_gasto} seg.")


def valida_no_responsavel_emissao(df_colunas, base_info):
    """
        Esta função valida se o numero da coluna no_responsavel_emisao é nulo
        caso a coluna codigo_diploma_certificado não esteja prenchida.

        Colunas validadas:

            66. no_responsavel_emissao

        Colunas auxiliares:

            55. codigo_diploma_certificado

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_no_responsavel_emissao( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_no_responsavel_emissao]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    coluna = 'no_responsavel_emissao'
    print(coluna)
    categoria = buscar_categoria(coluna)
    cod_coluna = buscar_cod_coluna(coluna)

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])
        codigo_diploma_certificado = linha['codigo_diploma_certificado']
        no_responsavel_emissao = linha['no_responsavel_emissao']

        if (type(codigo_diploma_certificado) != type(np.nan)):
           if (type(no_responsavel_emissao) == type(np.nan)):
               novaLinha = [[codUni, coluna, 'vazio_não_conforme_a_regra', categoria, cod_coluna]]
               resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

    if len(resultado.index) != 0:
        nome_funcao = 'valida_no_responsavel_emissao_' + coluna
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_no_responsavel_emissao'] = tempo_gasto
    print(f"Tempo total da Função (valida_no_responsavel_emissao): {tempo_gasto} seg.")


def valida_numero_cpf(df_colunas, base_info):
    """
        Esta função valida o número do CPF de acordo com a coluna
        situação do CPF. Se campo ‘situacao_cpf’ for "COM CPF",
        deve conter um número de CPF válido.
        Se campo 'situacao_cpf" for "SEM CPF", deve estar vazio

        Colunas validadas:

            35. numero_cpf

        Colunas auxiliares:

            34. situacao_cpf

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_numero_cpf( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_numero_cpf]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    coluna = 'numero_cpf'
    print(coluna)
    categoria = buscar_categoria(coluna)
    cod_coluna = buscar_cod_coluna(coluna)

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])
        situacao_cpf = linha['situacao_cpf']
        cpf = linha[coluna]

        if (type(situacao_cpf) == type(np.nan)):
            novaLinha = [[codUni, 'situacao_cpf', "NaN", categoria, cod_coluna]]
            resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
        else:
            if situacao_cpf == 'COM CPF':
                if (type(cpf) != type(np.nan)):
                    # Acrescentar o 0 nos cpf que possui 10 digitos.
                    # zfill: acrescenta o 0 ao inicio do cpf que contém 10 digitos.
                    if (len(cpf) == 10):
                        cpf = cpf.zfill(11)

                    # cpf_valido: retorna True se o CPF informado é valido.
                    if not verifica_cpf(cpf):
                        novaLinha = [[codUni, coluna, 'cpf_invalido', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
                else:
                    novaLinha = [[codUni, coluna, 'situacao_com_cpf_cpf_vazio', categoria, cod_coluna]]
                    resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

            elif situacao_cpf == 'SEM CPF':
                if (type(cpf) != type(np.nan)):
                    novaLinha = [[codUni, coluna, 'situacao_sem_cpf_cpf_preenchido', categoria, cod_coluna]]
                    resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
            else:
                # nesse ponto, situacao_cpf pode ter um valor diferente de 'COM CPF' e 'SEM CPF'
                novaLinha = [[codUni, 'situacao_cpf', situacao_cpf, categoria, cod_coluna]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

    if len(resultado.index) != 0:
        nome_funcao = 'valida_numero_cpf_' + coluna
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_numero_cpf'] = tempo_gasto
    print(f"Tempo total da Função (valida_numero_cpf): {tempo_gasto} seg.")


def valida_tipo_oferta(df_colunas, base_info):
    """
        Função para validar a coluna tipo_oferta que não pode ser nula, e validada
        de acordo com a coluna co_tipo_curso (3). Além disso, a coluna tipo_oferta
        também pode ter o caracter hífen (-) simbolizando nada preenchido, e ainda
        seus valores, quando não nulos, estar compreendidos dentro da lista:
        ['INTEGRADO', 'CONCOMITANTE', 'SUBSEQUENTE', 'PROJETA - INTEGRADO',
        'PROJETA - CONCOMITANTE'].

        Colunas validadas:

            33. tipo_oferta

        Colunas auxiliares:

            15. co_tipo_curso

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_tipo_oferta( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_tipo_oferta]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    coluna = 'tipo_oferta'
    print(coluna)
    categoria = buscar_categoria(coluna)
    cod_coluna = buscar_cod_coluna(coluna)

    lista_ofertas = ['INTEGRADO', 'CONCOMITANTE', 'SUBSEQUENTE', 'PROJETA - INTEGRADO', 'PROJETA - CONCOMITANTE']

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])
        tipo_oferta = linha[coluna]

        if (type(tipo_oferta) == type(np.nan)):
            novaLinha = [[codUni,  tipo_oferta, "NaN", categoria, cod_coluna]]
            resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
        else:
            co_tipo_curso = linha['co_tipo_curso']
            # Se o codigo nao for 1, 3, 5 (lista de cursos tecnicos e tecnologicos),
            # e a coluna tipo_oferta estiver preenchida diferente de - .
            # gerar inconsistencia
            if co_tipo_curso == 3:
                  if not tipo_oferta.upper() in lista_ofertas:
                      novaLinha = [[codUni, coluna, 'não_conforme_a_regra', categoria, cod_coluna]]
                      resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
            else:
              if tipo_oferta != '-':
                      novaLinha = [[codUni, coluna, 'diferente_hifen_e_não_conforme_a_regra', categoria, cod_coluna]]
                      resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

    if len(resultado.index) != 0:
        nome_funcao = 'valida_tipo_oferta_' + coluna
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_tipo_oferta'] = tempo_gasto
    print(f"Tempo total da Função (valida_tipo_oferta): {tempo_gasto} seg.")

# Essa função comentada foi feita a junção dela com a data_geracao_codigo_diploma
# que se chama valida_diploma_certificado
#
# def valida_codigo_diplomacertificado(df_colunas, base_info):
#     """
#         Função que valida a coluna "codigo_diploma_certificado" se ela é um campo
#         alfanumérico. Se data_geracao_codigo_diplomacertificado estiver preenchido,
#         a coluna "codigo_diploma_certificado" não pode ser nulo.

#         Colunas validadas:

#             55. codigo_diploma_certificado

#         Colunas auxiliares:

#             54. data_geracao_codigo_diplomacertificado

#         Parameters
#         ----------
#         df_colunas : DataFrame
#             Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
#             demais colunas a serem processadas.
#         base_info : Dict
#             Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

#         Returns
#         -------
#         None

#         Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
#         [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

#         Examples
#         --------
#         >>> valida_codigo_diploma_certificado(df_colunas, base_info )
#     """

#     print("Iniciando os processos da função: [valida_codigo_diploma_certificado]");
#     start = time.perf_counter()

#     resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
#     df_dict = df_colunas.to_dict('records')

#     coluna = 'codigo_diploma_certificado'
#     print(coluna)
#     categoria = buscar_categoria('codigo_diploma_certificado')
#     cod_coluna = buscar_cod_coluna('codigo_diploma_certificado')

#     for linha in tqdm(df_dict):
#         codUni = str(linha['codigo_universal_linha'])
#         codigo_diploma_certificado = str(linha['codigo_diploma_certificado'])
#         data_geracao_codigo_diplomacertificado = str(linha['data_geracao_codigo_diplomacertificado'])

#         if (type(data_geracao_codigo_diplomacertificado) != type(np.nan)):
#             if(type(codigo_diploma_certificado) == type(np.nan)):
#                 novaLinha = [[codUni, coluna, 'Nan', categoria, cod_coluna]]
#                 resultado = pd.concat([pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

#     if len(resultado.index) != 0:
#         nome_funcao = 'valida_codigo_diploma_certificado_' + 'codigo_diploma_certificado'
#         data = data_hora_atual_como_string()
#         caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
#         gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
#         resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
#     else:
#         print("Nenhuma inconsistência encontrada na coluna: " + coluna)

#     end = time.perf_counter()
#     tempo_gasto = round(end - start, 2)
#     global_tempo_gasto_funcoes['valida_codigo_diploma_certificado'] = tempo_gasto
#     print(f"Tempo total da Função (valida_codigo_diploma_certificado): {tempo_gasto} seg.")


def valida_diploma_certificado(df_colunas, base_info):
    """
        Está função deve validar a coluna data_geracao_codigo_diplomacertificado
        Campo do tipo data e hora no formato dd/mm/YY HH:mm.
        Se codigo_diploma_certificado estiver preenchido, não pode ser nulo
        Campo alfanumérico. Se data_geracao_codigo_diplomacertificado estiver
        preenchido, não pode ser nulo

        Colunas validadas:

            54. data_geracao_codigo_diplomacertificado
            55. codigo_diploma_certificado

        Colunas auxiliares:

            15. co_tipo_curso
            29. situacao_matricula

        Validação da coluna "codigo_diploma_certificado" se ela é um campo
        alfanumérico. Se data_geracao_codigo_diplomacertificado estiver preenchido,
        a coluna "codigo_diploma_certificado" não pode ser nulo.

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_diploma_certificado(df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_diploma_certificado]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    coluna_codigo_diploma = 'data_geracao_codigo_diplomacertificado'
    coluna_data_geracao = 'codigo_diploma_certificado'
    print(coluna_data_geracao)             #como essa função são duas colunas que serão analisadas, puxei as informações de cada uma
    print(coluna_codigo_diploma)
    categoria_data_geracao = buscar_categoria(coluna_data_geracao)
    categoria_codigo_diploma = buscar_categoria(coluna_codigo_diploma)
    cod_coluna_data_geracao = buscar_cod_coluna(coluna_data_geracao)
    cod_coluna_codigo_diploma = buscar_cod_coluna(coluna_codigo_diploma)

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])
        data_geracao_codigo_diploma_certificado = str(linha['data_geracao_codigo_diplomacertificado'])
        codigo_diploma_certificado = (linha['codigo_diploma_certificado'])
        co_tipo_curso = (linha['co_tipo_curso'])
        situacao_matricula = str(linha['situacao_matricula'])

        # Verificando se a data é valida usando a função auxiliar verifica_data_
        valida_data_geracao_codigo_diploma_certificado = verifica_data_(data_geracao_codigo_diploma_certificado)

        if (type(data_geracao_codigo_diploma_certificado) != (type(str(np.nan)))):  # como as datas são convertidas para str, converte também o tipo do  np.nan, para verificar se possui vazio
            if not valida_data_geracao_codigo_diploma_certificado:
                novaLinha = [[codUni, coluna_data_geracao, 'data_invalida', categoria_data_geracao, cod_coluna_data_geracao]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
            else:
                if (type(codigo_diploma_certificado) == type(np.nan) or (codigo_diploma_certificado == '-')):
                    novaLinha = [[codUni, coluna_codigo_diploma, 'data_geracao_existe_codigo_diploma_não',  categoria_codigo_diploma, cod_coluna_codigo_diploma]]
                    resultado = pd.concat([pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)
                else:
                    if (co_tipo_curso != 3) and (situacao_matricula.lower() != 'concluida'):
                        novaLinha = [[codUni, coluna_codigo_diploma, 'diferente_1_ou_3_situacao_nao_concluida', categoria_codigo_diploma, cod_coluna_codigo_diploma]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)
        else:
            if (type(codigo_diploma_certificado) != type(np.nan) or (codigo_diploma_certificado != '-')):
                novaLinha = [[codUni, coluna_codigo_diploma, 'data_geracao_não_existe_codigo_diploma_sim', categoria_codigo_diploma, cod_coluna_codigo_diploma]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

    if len(resultado.index) != 0:
        nome_funcao = 'valida_diploma_certificado_' + 'data_geracao_codigo_diplomacertificado_' + 'codigo_diploma_certificado'
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna_codigo_diploma)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_diploma_certificado'] = tempo_gasto
    print(f"Tempo total da Função (valida_diploma_certificado): {tempo_gasto} seg.")


def valida_bool_nao_nulo(df_colunas, base_info):
    """
        Esta função deve validar se as colunas passadas são do tipo boolean (foi
        exportada como SIM ou NÃO) e se NÃO estão vazias, caso contrário será
        gerado inconsistências.

        Colunas validadas:

            19. experimental
            30. contrato_aprendizagem
            32. atestado_baixa_renda
            45. sg_etec
            60. st_ativo


        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_bool_nao_nulo( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_bool_nao_nulo]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]

                if (type(valor) == type(np.nan)):
                    novaLinha = [[codUni, coluna, 'NaN', categoria, cod_coluna]]
                    resultado = pd.concat([pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)
                else:
                    # Se a função retorna False. o valor boleano é invalido
                    if not verifica_boleano(valor):
                        novaLinha = [[codUni, coluna, "boleano_invalido", categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

            if len(resultado.index) != 0:
                nome_funcao = 'valida_bool_não_nulo_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_bool_nao_nulo'] = tempo_gasto
    print(f"Tempo total da Função (valida_bool_nao_nulo): {tempo_gasto} seg.")


def valida_bool(df_colunas, base_info):
    """
        Função para validar se a coluna é do tipo boleano. SIM OU NÃO
        podendo ser nulo

        Colunas validadas:

            37. carga
            46. sg_proeja
            59. st_deferimento_ue

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_bool( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_bool]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]

                if (type(valor) != type(np.nan)):
                    if not verifica_boleano(valor):
                        novaLinha = [[codUni, coluna, "boleano_invalido", categoria, cod_coluna]]
                        resultado = pd.concat([pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

            if len(resultado.index) != 0:
                nome_funcao = 'valida_bool_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna:" + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_bool'] = tempo_gasto
    print(f"Tempo total da Função (valida_bool): {tempo_gasto} seg.")


def valida_tipo_cota(df_colunas, base_info):
    """
        Função para validar se o aluno possui cota ou não. O valor da coluna deve
        estar compreendido dentro dos valores: 'sem cota, com cota, sim, não, nan,
        none, NECESSIDADES ESPECIAIS, INDÍGENA, ESCOLA PUBLICA, COR/RAÇA, OLIMPÍADA,
        ZONA RURAL, QUILOMBOLA, ASSENTAMENTO'.

        Colunas validadas:

            31. tipo_cota

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_tipo_cota( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_tipo_cota]");
    start = time.perf_counter()

    # Respotas sobre a linha da coluna tipo_cota
    cota = 'SEM COTA sem cota COM COTA com cota SIM sim NAO NÃO nao não nan Nan none None NECESSIDADES ESPECIAIS INDÍGENA ESCOLA PUBLICA ESCOLA PÚBLICA COR/RAÇA Cor/Raça cor/raça OLIMPÍADA OLIMPIADA INDÍGENA INDIGENA ZONA RURAL QUILOMBOLA ASSENTAMENTO'
    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]
                if (type(valor) != type(np.nan)):
                    if not valor in cota:
                        novaLinha = [[codUni, coluna, 'boleano_invalido_lista_cota', categoria, cod_coluna]]
                        resultado = pd.concat([pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)
                else:
                   novaLinha = [[codUni, coluna, 'NaN', categoria, cod_coluna]]
                   resultado = pd.concat([pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

            if len(resultado.index) != 0:
                nome_funcao = 'valida_tipo_cota_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_tipo_cota'] = tempo_gasto
    print(f"Tempo total da Função (valida_tipo_cota): {tempo_gasto} seg.")


def valida_sg_brasilpro(df_colunas, base_info):
    """
        publica estadual

        Função para validar a coluna sg_brasilpro, na qual o campo é booleano e
        não nulo. Se o campo 'ano_do_convenio_brasilpro' está preenchido, deve
        constar "sim", se estiver vazio deve constar "não" como seu conteúdo.

        Colunas validadas:

            47. sg_brasilpro

        Colunas auxiliares:

            48. ano_convenio_brasilpro

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_sg_brasilpro( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_sg_brasilpro]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    coluna = 'sg_brasilpro'
    print(coluna)
    categoria = buscar_categoria(coluna)
    cod_coluna = buscar_cod_coluna(coluna)

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])
        ano_convenio_brasil_pro = linha['ano_convenio_brasilpro']
        sg_brasilpro = linha['sg_brasilpro']

        # verifica se o ano_convenio_brasil_pro é diferente de nulo,
        # Em caso afirmativo, o sg_brasilpro deve estar preenchido com sim
        if (type(ano_convenio_brasil_pro) != type(np.nan)):
            if (sg_brasilpro) != 'sim' or (type(sg_brasilpro) == type(np.nan)):
                novaLinha = [[codUni, 'sg_brasilpro', 'não_conforme_a_regra', categoria, cod_coluna]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
        else:
            if (sg_brasilpro) != 'não' or (type(sg_brasilpro) == type(np.nan)):
                novaLinha = [[codUni, 'sg_brasilpro', 'não_conforme_a_regra', categoria, cod_coluna]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

    if len(resultado.index) != 0:
        nome_funcao = 'valida_sg_brasilpro_' + coluna
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_sg_brasilpro'] = tempo_gasto
    print(f"Tempo total da Função (valida_sg_brasilpro): {tempo_gasto} seg.")

#OBS essa função não é mais necessária, pois agora a dt_data_inicio faz parte da valida_data_nao_nulo
# def valida_dt_data_inicio(df_colunas, base_info):
#     """
#         Função para validar a coluna dt_data_inicio, a qual não pode ser nula,
#         precisa ser uma data válida e deve ser menor ou igual (<=) que a dt_deferimento_ue.

#         Colunas validadas:

#             9. dt_data_inicio

#         Colunas auxiliares:

#             3. dt_deferimento_ue

#         Parameters
#         ----------
#         df_colunas : DataFrame
#             Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
#             demais colunas a serem processadas.
#         base_info : Dict
#             Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

#         Returns
#         -------
#         None

#         Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
#         [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

#         Examples
#         --------
#         >>> valida_dt_data_inicio( df_colunas, base_info )
#     """

#     print("Iniciando os processos da função: [valida_dt_data_inicio]");
#     start = time.perf_counter()

#     resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
#     df_dict = df_colunas.to_dict('records')

#     coluna = "dt_data_inicio"
#     print(coluna)
#     categoria = buscar_categoria(coluna)
#     cod_coluna = buscar_cod_coluna(coluna)

#     for linha in tqdm(df_dict):
#         codUni = str(linha['codigo_universal_linha'])

#         # Pegando os valores da linha dt_data_inicio
#         data_inicio = linha[coluna]

#         # Pegando os valores da linha dt_deferimento ue
#         data_deferimento_ue = linha['dt_deferimento_ue']

#         # Verificando se a data é nula
#         if (type(data_inicio) == type(np.nan)):
#             novaLinha = [[codUni, coluna, 'Nan', categoria, cod_coluna]]
#             resultado = pd.concat([pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)
#         else:
#             # Chamada da função verifica_data_valida para verificar se valor é uma data valida
#             data_inicio_valida = verifica_data_valida(data_inicio)
#             data_deferimento_ue_valida = verifica_data_valida(data_deferimento_ue)

#             # Para Gerar incosistencia caso a data nao seja valida e caso nao esteja no formato de uma data
#             if (data_inicio_valida == False):
#                 novaLinha = [[codUni, coluna, 'data_invalida', categoria, cod_coluna]]
#                 resultado = pd.concat([pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

#             if (data_inicio_valida != False and data_deferimento_ue_valida != False):
#                 # Comparando as datas. De acordo com o documento de exploração de dados,
#                 # a data_inicio nao pode ser inferior a data deferimento ue
#                 if (data_inicio_valida <= data_deferimento_ue_valida):
#                     novaLinha = [[codUni,  coluna, 'data_inicio_inferior', categoria, cod_coluna]]
#                     resultado = pd.concat([pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

#     if len(resultado.index) != 0:
#         nome_funcao = 'valida_dt_data_inicio_' + coluna
#         data = data_hora_atual_como_string()
#         caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
#         gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
#         resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
#     else:
#         print("Nenhuma inconsistência encontrada na coluna: " + coluna)

#     end = time.perf_counter()
#     tempo_gasto = round(end - start, 2)
#     global_tempo_gasto_funcoes['valida_dt_data_inicio'] = tempo_gasto
#     print(f"Tempo total da Função (valida_dt_data_inicio): {tempo_gasto} seg.")


def valida_dt_deferimento_curso(df_colunas, base_info):
    """
        Função para validar a coluna dt_deferimento_curso,
        Deve conter uma data válida não nula e no formato dd/mm/aaaa
        e deve ser maior ou igual à dt_deferimento_ue

        Colunas principais na validação:

            14. dt_deferimento_curso

        Colunas auxiliares:

            3. dt_deferimento_ue

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_dt_deferimento_curso( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_dt_deferimento_curso]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    coluna = "dt_deferimento_curso"
    print(coluna)
    categoria = buscar_categoria(coluna)
    cod_coluna = buscar_cod_coluna(coluna)

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])

        # Pegando os valores da linha dt_deferimento_ue
        dt_deferimento_ue = linha['dt_deferimento_ue']

        # Pegando os valores da linha dt_deferimento_curso
        data_deferimento_ue_curso = linha[coluna]

        # Verificando se a data deferimento ue está vazia
        if (type(data_deferimento_ue_curso) == type(np.nan)):
            novaLinha = [[codUni, coluna, 'NaN', categoria, cod_coluna]]
            resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
        else:
            # Chamada da função verifica_data para verificar se a data_deferimento_ue_curso_valida é uma data valida
            dt_deferimento_ue_valida = verifica_data_valida(dt_deferimento_ue)
            data_deferimento_ue_curso_valida = verifica_data_valida(data_deferimento_ue_curso)

            # Para Gerar incosistencia caso a data nao seja valida e caso nao esteja no formato de uma data
            if (data_deferimento_ue_curso_valida == False):
                # print(data_deferimento_ue_curso_valida)
                novaLinha = [[codUni, coluna, 'data_invalida', categoria, cod_coluna]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
            else:
                if (dt_deferimento_ue_valida != False and data_deferimento_ue_curso_valida != False):
                    # Comparando as datas. De acordo com o documento exploração de dados, a data_deferimento_ue_curso_valida nao pode ser superior a data_inicio
                    if (data_deferimento_ue_curso_valida < dt_deferimento_ue_valida):
                        novaLinha = [[codUni, coluna, 'dt_deferimento_ue_superior_não_conforme_a_regra', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

    if len(resultado.index) != 0:
        nome_funcao = 'valida_dt_deferimento_curso_' + coluna
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_dt_deferimento_curso'] = tempo_gasto
    print(f"Tempo total da Função (valida_dt_deferimento_curso): {tempo_gasto} seg.")


def valida_dt_cadastro_aluno_sistema(df_colunas, base_info):
    """
        Função para verificar a coluna 'dt_cadastro_aluno_sistema', na qual deve
        conter uma data válida não nula e no formato dd/mm/aaaa, e deve ser
        maior ou igual a 'dt_cadastro_ciclo'.

        Colunas validadas:

            27. dt_cadastro_aluno_sistema

        Colunas auxiliares:

            21. dt_cadastro_ciclo

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_dt_cadastro_aluno_sistema( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_dt_cadastro_aluno_sistema]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    coluna = "dt_cadastro_aluno_sistema"
    print(coluna)
    categoria = buscar_categoria(coluna)
    cod_coluna = buscar_cod_coluna(coluna)

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])
        dt_cadastro_aluno_sistema = str(linha['dt_cadastro_aluno_sistema'])
        dt_cadastro_ciclo = str(linha['dt_cadastro_ciclo'])

        if (type(dt_cadastro_aluno_sistema) == type(np.nan)):
            novaLinha = [[codUni, 'dt_cadastro_aluno_sistema', 'Nan', categoria, cod_coluna]]
            resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
        else:
            valida_data_dt_cadastro_aluno_sistema = verifica_data_nulo(dt_cadastro_aluno_sistema)
            valida_dt_cadastro_ciclo = verifica_data_nulo(dt_cadastro_ciclo)

            if (valida_data_dt_cadastro_aluno_sistema == False):
                novaLinha = [[codUni, 'dt_cadastro_aluno_sistema', 'data_invalida', categoria, cod_coluna]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)
            else:
                if (valida_data_dt_cadastro_aluno_sistema != False and  valida_dt_cadastro_ciclo  != False):
                    if (dt_cadastro_aluno_sistema < dt_cadastro_ciclo):
                        novaLinha = [[codUni, 'dt_cadastro_aluno_sistema', 'dt_cadastro_aluno_sistema_inferior_não_conforme_a_regra', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

    if len(resultado.index) != 0:
        nome_funcao = 'valida_dt_cadastro_aluno_sistema_' + coluna
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_dt_cadastro_aluno_sistema'] = tempo_gasto
    print(f"Tempo total da Função (valida_dt_cadastro_aluno_sistema): {tempo_gasto} seg.")


def valida_dt_cadastro_ciclo(df_colunas, base_info):
    """
        Função para verificar a coluna 'dt_cadastro_ciclo', na qual deve
        conter uma data válida não nula e no formato dd/mm/aaaa, e deve ser
        maior ou igual  que 'dt_deferimento_curso'.

        Colunas validadas:

            21. dt_cadastro_ciclo

        Colunas auxiliares:

            14. dt_deferimento_curso

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_dt_cadastro_aluno_sistema( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_dt_cadastro_aluno_sistema]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    coluna = "dt_cadastro_ciclo"
    print(coluna)
    categoria = buscar_categoria(coluna)
    cod_coluna = buscar_cod_coluna(coluna)

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])
        dt_cadastro_ciclo = str(linha['dt_cadastro_ciclo'])
        dt_deferimento_curso = str(linha['dt_deferimento_curso'])

        if (type(dt_cadastro_ciclo) == type(np.nan)):
            novaLinha = [[codUni, 'dt_cadastro_ciclo', 'Nan', categoria, cod_coluna]]
            resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
        else:
            valida_data_dt_cadastro_ciclo = verifica_data_nulo(dt_cadastro_ciclo)
            valida_data_dt_deferimento_curso = verifica_data_nulo(dt_deferimento_curso)

            if (valida_data_dt_cadastro_ciclo == False):
                novaLinha = [[codUni, 'dt_cadastro_ciclo', 'data_invalida', categoria, cod_coluna]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)
            else:
                if (valida_data_dt_cadastro_ciclo != False and valida_data_dt_deferimento_curso != False):
                    if (dt_cadastro_ciclo < dt_deferimento_curso):
                        novaLinha = [[codUni, 'dt_cadastro_ciclo', 'dt_cadastro_ciclo_inferior_não_conforme_a_regra', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

    if len(resultado.index) != 0:
        nome_funcao = 'valida_dt_cadastro_ciclo_' + coluna
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_dt_cadastro_ciclo'] = tempo_gasto
    print(f"Tempo total da Função (valida_dt_cadastro_ciclo): {tempo_gasto} seg.")

def valida_dt_data_fim_previsto(df_colunas, base_info):
    """
        Esta função valida a coluna dt_data_fim_previsto. Deve conter uma data válida e no
        formato dd/mm/aaaa e ser maior ou igual a data de início (dt_data_inicio)
        se co_tipo_curso for "2" ou maior que data de início (dt_data_inicio) para os demais
        co_tipo_curso, pode ser nulo

        Colunas validadas:

            10. dt_data_fim_previsto

        Colunas auxiliares

            9. dt_data_inicio
            15. co_tipo_curso

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_dt_data_fim_previsto( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_dt_data_fim_previsto]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    coluna = "dt_data_fim_previsto"
    print(coluna)
    categoria = buscar_categoria(coluna)
    cod_coluna = buscar_cod_coluna(coluna)

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])

        # Pegando os valores da linha dt_data_inicio
        data_inicio = str(linha['dt_data_inicio'])

        #co_tipo_curso  para analisar a linha
        co_tipo_curso = (linha['co_tipo_curso'])

        # Pegando os valores da linha dt_data_fim_previsto
        data_fim_previsto = str(linha[coluna])

        # Chamada da função verifica_data para verificar se a da linha dt_data_fim_previsto é uma data valida
        data_inicio_valida = verifica_data_valida(data_inicio)
        data_fim_previsto_valida = verifica_data_valida(data_fim_previsto)

        # Para Gerar incosistencia caso a data nao seja valida e caso nao esteja no formato de uma data
        if (data_fim_previsto_valida == False):
            novaLinha = [[codUni, coluna, 'data_invalida', categoria, cod_coluna]]
            resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
        else:
            if(co_tipo_curso ==2): # se o codigo for igual a 2 segundo a documentação se data_fim_previsto for menor ou igual a dt_data_inico gera inconsistências
              if (data_inicio_valida != False and data_fim_previsto_valida != False): #Para comparar as datas, as duas tem que ser validas, por isso essa verificação
                  # Comparando as datas. De acordo com o document exploração de dados,
                  # a data_fim_prevista nao pode ser inferior a data_inicio
                  if (data_fim_previsto_valida < data_inicio_valida):
                      novaLinha = [[codUni, coluna, 'dt_data_fim_previsto_inferior_data_inicio', categoria, cod_coluna]]
                      resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
            else:
              if (data_inicio_valida != False and data_fim_previsto_valida != False): #Para comparar as datas, as duas tem que ser validas, por isso essa verificação
                  # Comparando as datas. De acordo com o document exploração de dados,
                  # a data_fim_prevista nao pode ser inferior a data_inicio
                  if (data_fim_previsto_valida <= data_inicio_valida):
                      novaLinha = [[codUni, coluna, 'dt_data_fim_previsto_inferior_ou_igual_data_inicio', categoria, cod_coluna]]
                      resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )


    if len(resultado.index) != 0:
        nome_funcao = 'valida_dt_data_fim_previsto_' + coluna
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_dt_data_fim_previsto'] = tempo_gasto
    print(f"Tempo total da Função (valida_dt_data_fim_previsto): {tempo_gasto} seg.")


def valida_data_nao_nulo(df_colunas, base_info):
    """
        Esta função valida as colunas que não podem ter valores nulos e devem
        possuir o formato dd/mm/aaaa.

        Colunas validadas:

            9. dt_data_inicio
            3. dt_deferimento_ue
            21. dt_cadastro_ciclo
            26. dt_nascimento_aluno
            40. dt_ocorrencia_ciclo
            41. dt_ocorrencia_matricula
            42. data_ultima_alteracao

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_data_nao_nulo( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_data_nao_nulo]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]

                if (type(valor) == type(np.nan)):
                    novaLinha = [[codUni, coluna, 'Nan', categoria, cod_coluna]]
                    resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
                else:
                    # Fazendo a chamada da função que verifica se a data é valida
                    valida_data_nao_nulo = verifica_data(valor)

                    if (valida_data_nao_nulo == False):
                        novaLinha = [[codUni, coluna, 'data_invalida', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

            if len(resultado.index) != 0:
                nome_funcao = 'valida_data_não_nulo_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_data_nao_nulo'] = tempo_gasto
    print(f"Tempo total da Função (valida_data_nao_nulo): {tempo_gasto} seg.")


def valida_data(df_colunas, base_info):
    """
        Função para validar as colunas com datas que devem conter uma data válida
        e no formato dd/mm/yyyy e pode ser nulo.

        Colunas validadas:

            52. dt_data_finalizado

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_data( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_data]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = str(linha[coluna])

                if not verifica_data_nulo(valor):
                    novaLinha = [[codUni, coluna,'data_invalida', categoria, cod_coluna]]
                    resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

            if len(resultado.index) != 0:
                nome_funcao = 'valida_data_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_data'] = tempo_gasto
    print(f"Tempo total da Função (valida_data): {tempo_gasto} seg.")

#@Função precisa ajustar para preencher vazios com nan na parte do processamento principal
def valida_ano_convenio_brasil_pro(df_colunas, base_info):
    """
        Função para validar a coluna ano_convenio_brasilpro no formato
        data (yyyy) e pode ser nulo. O ano salvo na coluna não pode ser maior
        que a data atual.

        Colunas validadas:

            48. ano_convenio_brasilpro

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_ano_convenio_brasil_pro( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_ano_convenio_brasil_pro]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = (linha['ano_convenio_brasilpro'])
                #x = valor.replace('', "teste") #codigo criado para testar conversão de espaços vazios
                #print(x)
                # passando o valor para função que verifica se o ano é valido
                if(type(valor) != type(np.nan)):
                  valida_data_ano = verifica_data_ano_valida(valor)
                  if not verifica_data_ano_valida(valor):
                      novaLinha = [[codUni, coluna, 'ano_invalido', categoria, cod_coluna]]
                      resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)
                  else:
                      data_hora_corrente = datetime.now()
                      data_corrente = data_hora_corrente.date()
                      ano_corrente = data_corrente.strftime("%Y")
                      ano = int(ano_corrente)
                      valor = int(valor)
                          # Verificando se o ano é maior que o ano atual, se for gerar inconsistencia.
                      if (valor > ano):
                          novaLinha = [[codUni, coluna, 'ano_maior_que_o_ano_atual_não_conforme_a_regra', categoria, cod_coluna]]
                          resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True)

            if len(resultado.index) != 0:
                nome_funcao = 'valida_ano_convenio_brasil_pro_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_ano_convenio_brasil_pro'] = tempo_gasto
    print(f"Tempo total da Função (valida_ano_convenio_brasil_pro): {tempo_gasto} seg.")
"""

#
# Essa função comentada foi feita a junção dela com a codigo_diploma_certificado
# que se chama valida_diploma_certificado
#
# def valida_data_geracao_codigo_diploma_certificado(df_colunas, base_info):
#     """
#         Está função deve validar a coluna data_geracao_codigo_diplomacertificado
#         que deve ser do tipo data dd/mm/aaaa e pode ser nulo. No entanto, se a
#         coluna codigo_diploma_certificado estiver preenchida, a
#         data_geracao_codigo_diplomacertificado NÃO pode ser nula.

#         Colunas validadas:

#             54. data_geracao_codigo_diplomacertificado

#         Colunas auxiliares:

#             55. codigo_diploma_certificado

#         Parameters
#         ----------
#         df_colunas : DataFrame
#             Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
#             demais colunas a serem processadas.
#         base_info : Dict
#             Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

#         Returns
#         -------
#         None

#         Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
#         [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

#         Examples
#         --------
#         >>> valida_data_geracao_codigo_diploma_certificado( df_colunas, base_info )
#     """

#     print("Iniciando os processos da função: [valida_data_geracao_codigo_diploma_certificado]");
#     start = time.perf_counter()

#     resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
#     df_dict = df_colunas.to_dict('records')

#     coluna = 'data_geracao_codigo_diplomacertificado'
#     print(coluna)
#     categoria = buscar_categoria(coluna)
#     cod_coluna = buscar_cod_coluna(coluna)

#     for linha in tqdm(df_dict):
#         codUni = str(linha['codigo_universal_linha'])
#         codigo_diploma_certificado = str(linha['codigo_diploma_certificado'])
#         data_geracao_codigo_diploma_certificado = str(linha[coluna])

#         valida_data_geracao_codigo_diploma_certificado = verifica_data_(data_geracao_codigo_diploma_certificado)

#         # Se o codigo diploma existe, então a data_geracao_codigo_diploma_certificado deve existir
#         if (type(codigo_diploma_certificado) != type(np.nan)):
#             if (type(data_geracao_codigo_diploma_certificado) == type(np.nan)):
#                 novaLinha = [[codUni, coluna, 'data_geracao_nao_existe', categoria, cod_coluna]]
#                 resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
#             else:
#                 if not valida_data_geracao_codigo_diploma_certificado:
#                     novaLinha = [[codUni, coluna, 'data_geracao_invalida', categoria, cod_coluna]]
#                     resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

#         # Se a data_geracao_codigo_diploma_certificado está preenchida, então deve ser uma data válida.
#         if (type(data_geracao_codigo_diploma_certificado) != type(np.nan)):
#             if (type(codigo_diploma_certificado) == type(np.nan)):
#                 novaLinha = [[codUni, coluna,'codigo_diploma_nao_existe', categoria, cod_coluna]]
#                 resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

#     if len(resultado.index) != 0:
#         nome_funcao = 'valida_data_geracao_codigo_diploma_certificado_' + coluna
#         data = data_hora_atual_como_string()
#         caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
#         gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
#         resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
#     else:
#       print("Nenhuma inconsistência encontrada na coluna"+ coluna)

#     end = time.perf_counter()
#     tempo_gasto = round(end - start, 2)
#     global_tempo_gasto_funcoes['valida_data_geracao_codigo_diploma_certificado'] = tempo_gasto
#     print(f"Tempo total da Função (valida_data_geracao_codigo_diploma_certificado): {tempo_gasto} seg.")


def valida_dt_validacao_conclusao(df_colunas, base_info):
    """
        Função para validar a coluna dt_validacao_conclusao que deve conter uma
        data válida no formato dd/mm/aaaa e pode ser nulo.

        Caso a data_geracao_codigo_diploma exista a dt_validacao_conclusao
        deve, também, estar preenchida.

        Colunas validadas:

            53. dt_validacao_conclusao

        Colunas auxiliares:

            54. data_geracao_codigo_diploma_certificado

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_dt_validacao_conclusao( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_dt_validacao_conclusao]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    coluna = 'dt_validacao_conclusao'
    print(coluna)
    categoria = buscar_categoria(coluna)
    cod_coluna = buscar_cod_coluna(coluna)

    for linha in tqdm(df_dict):
        codUni = str(linha['codigo_universal_linha'])

        dt_validacao_conclusao = str(linha[coluna])
        data_geracao_codigo_diploma_certificado = str(linha['data_geracao_codigo_diplomacertificado'])

        if type(dt_validacao_conclusao) != type(np.nan):
            if not verifica_data_nulo(dt_validacao_conclusao):
                novaLinha = [[codUni, coluna, 'data_invalida', categoria, cod_coluna]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

        if type(data_geracao_codigo_diploma_certificado) != type(np.nan):
            if type(dt_validacao_conclusao) == type(np.nan):
                novaLinha = [[codUni, coluna, 'dt_geracao_existe_dt_validacao_não_existe_não_conforme_a_regra', categoria, cod_coluna]]
                resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

    if len(resultado.index) != 0:
        nome_funcao = 'valida_dt_validacao_conclusao_' + coluna
        data = data_hora_atual_como_string()
        caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
        gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
        resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
    else:
        print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_dt_validacao_conclusao'] = tempo_gasto
    print(f"Tempo total da Função (valida_dt_validacao_conclusao): {tempo_gasto} seg.")


def valida_nu_cnpj(df_colunas, base_info):
    """
        Função para validar o número do CNPJ
        Campo alfanumérico e pode ser nulo.
        Se estiver preenchido, precisa ser um CNPJ válido

        Colunas validadas:

            62. nu_cnpj

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_nu_cnpj( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_nu_cnpj]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha':
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):
                codUni = str(linha['codigo_universal_linha'])
                valor = linha[coluna]

                # Como essa função pode aceitar nulo, se o type valor for diferente do type do valor nulo, então
                # a função passa a validar se o cnpj é valido
                if (type(valor) != type(np.nan)):
                    if verifica_cnpj(valor):
                        novaLinha = [[codUni, coluna, 'cnpj_invalido', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

            if len(resultado.index) != 0:
                nome_funcao = 'valida_nu_cnpj_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_nu_cnpj'] = tempo_gasto
    print(f"Tempo total da Função (valida_nu_cnpj): {tempo_gasto} seg.")


def valida_co_endereco_co_ies(df_colunas, base_info):
    """
       Função para validar se o sistema de ensino é Federal com dependência
       administrativa privada, as colunas co_ies e co_endereco_e-mec não podem
       ser nulas e devem ser do tipo numéricas.

       Colunas validadas:

            63. co_ies
            64. co_endereco_e-mec

        Colunas auxiliares:

            4.  dependencia_admin
            5.  sistema_ensino

        Parameters
        ----------
        df_colunas : DataFrame
            Este parâmetro deve conter a coluna 'codigo_universal_linha' e as
            demais colunas a serem processadas.
        base_info : Dict
            Deve conter um dicionário no formato: {'id': base, 'nome': nome_base}

        Returns
        -------
        None

        Cria um dataframe com as inconsistências encontradas e gera um .csv com as colunas:
        [codigo_universal_linha, coluna, tipo_inconsistencia, categoria, cod_coluna]

        Examples
        --------
        >>> valida_co_endereco_co_ies( df_colunas, base_info )
    """

    print("Iniciando os processos da função: [valida_co_endereco_co_ies]");
    start = time.perf_counter()

    resultado = pd.DataFrame( columns = ['codigo_universal_linha', 'coluna', 'tipo_inconsistencia', 'categoria', 'cod_coluna'] )
    df_dict = df_colunas.to_dict('records')

    for coluna in df_colunas.columns:
        if coluna != 'codigo_universal_linha' and (coluna in ['co_ies', 'co_endereco_e_mec']):
            print(coluna)
            categoria = buscar_categoria(coluna)
            cod_coluna = buscar_cod_coluna(coluna)

            for linha in tqdm(df_dict):

                codUni = str(linha['codigo_universal_linha'])
                sistema_ensino = str(linha['sistema_ensino'])
                dependencia_admin = str(linha['dependencia_admin'])
                valor = linha[coluna]

                if (sistema_ensino == 'Federal' and dependencia_admin == 'Privada'):
                    if (type(valor) == type(np.nan)):
                        novaLinha = [[codUni, coluna, 'vazio_não_conforme_a_regra', categoria, cod_coluna]]
                        resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )
                    else:
                        if not isinstance(valor, int) and not valor.isnumeric():
                            novaLinha = [[codUni, coluna, 'tipo_não_valido', categoria, cod_coluna]]
                            resultado = pd.concat( [pd.DataFrame(novaLinha, columns = resultado.columns), resultado], ignore_index = True )

            if len(resultado.index) != 0:
                nome_funcao = 'valida_co_endereco_co_ies_' + coluna
                data = data_hora_atual_como_string()
                caminho_arquivo = DIR_INCONSISTENCIAS + "/" + base_info["id"] + "_" + nome_funcao + "_" + data
                gerar_csv_inconsistencia(resultado, nome_funcao, caminho_arquivo)
                resultado.drop(resultado.index, inplace = True) # limpa o df para a próxima iteração
            else:
                print("Nenhuma inconsistência encontrada na coluna: " + coluna)

    end = time.perf_counter()
    tempo_gasto = round(end - start, 2)
    global_tempo_gasto_funcoes['valida_co_endereco_co_ies'] = tempo_gasto
    print(f"Tempo total da Função (valida_co_endereco_co_ies): {tempo_gasto} seg.")


'''
    FIM: ↑  Funções para limpeza de cada coluna  ↑
'''



'''
    INICIO: ↓  Funções que auxiliam nos processos  ↓
'''
def imprimir_exemplo_informacao_base_dados(df_base_dados):
    """
        Função para mostrar os tipos de dados de cada coluna e extrair
        um exemplo de informação

        Parameters
        ----------
        df_base_dados : DataFrame
            O parâmetro 'df_base_dados' de conter um DataFrame convertido da
            base de dados do SISTEC.

        Returns
        -------
            Esta função não possui retorno, apenas imprime no console.

        Examples
        --------
        >>> imprimir_exemplo_informacao_base_dados(df_base_dados)
        Out:
        ----------------------------------------------------
        Tipo        Nome da coluna          Exemplo
        ----------------------------------------------------
        int         co_unidade_ensino       1254
        int         co_simec                14
        str         unidade_ensino          senac
        ...
        ----------------------------------------------------

    """
    template = "%-15s %-40s %s"
    print(template % ("Tipo", "Nome da coluna", "Exemplo"))
    print("-" * 100)
    for c in df_base_dados.columns:
        print(template % (df_base_dados[c].dtype, c, df_base_dados[c].iloc[1]) )


def analisar_colunas(df_base_dados):
    """
        Função para analisar cada coluna do DataFrame e resumir os tipos de
        dados encontrados em cada coluna.

        Parameters
        ----------
        df_base_dados : DataFrame
            O parâmetro 'df_base_dados' de conter um DataFrame convertido da
            base de dados do SISTEC.

        Returns
        -------
            Esta função não possui retorno, apenas imprime no console.

        Examples
        --------
        >>> analisar_colunas(df_base_dados)

    """
    dfCounts = pd.DataFrame({'valor_unico':[], 'quantidade':[], 'coluna_analisada':[]})

    for c in df_base_dados.columns:
        temp = None
        temp = pd.DataFrame( df_base_dados[c].value_counts(dropna = False) )
        temp['coluna_analisada'] = c
        temp = temp.reset_index()
        temp.columns = ['valor_unico', 'quantidade', 'coluna_analisada']
        dfCounts = dfCounts.append(temp, ignore_index = True)
        print(df_base_dados[c].value_counts(dropna = False))
        print('-' * 50)
        print('\n\r')

    dfCounts = dfCounts[['coluna_analisada', 'valor_unico', 'quantidade']]
    print(dfCounts)


def buscar_categoria(coluna):
    """
        Função para buscar a categoria da coluna informada.

        Parameters
        ----------
        coluna : str
            Deve conter o nome da coluna que se deseja encontrar a categoria.

        Returns
        -------
        Str

        Examples
        --------
        >>> temp = buscar_categoria ( 'municipio' )
    """

    colunaCategoria = {'co_unidade_ensino' : 'Instituição',
                       'unidade_ensino' : 'Instituição',
                       'dt_deferimento_ue' : 'Instituição',
                       'dependencia_admin' : 'Instituição',
                       'sistema_ensino' : 'Instituição',
                       'municipio' : 'Instituição',
                       'estado' : 'Instituição',
                       'co_ciclo_matricula' : 'Oferta',
                       'dt_data_inicio' : 'Oferta',
                       'dt_data_fim_previsto' : 'Oferta',
                       'ds_eixo_tecnologico' : 'Curso',
                       'co_curso' : 'Curso',
                       'no_curso' : 'Curso',
                       'dt_deferimento_curso' : 'Curso',
                       'co_tipo_curso' : 'Curso',
                       'tipo_curso' : 'Curso',
                       'co_tipo_nivel' : 'Curso',
                       'ds_tipo_nivel' : 'Curso',
                       'experimental' : 'Curso',
                       'nome_ciclo' : 'Oferta',
                       'dt_cadastro_ciclo' : 'Oferta',
                       'carga_horaria' : 'Oferta',
                       'programa_associado' : 'Matrícula',
                       'nome_aluno' : 'Matrícula',
                       'sg_sexo' : 'Matrícula',
                       'dt_nascimento_aluno' : 'Matrícula',
                       'dt_cadastro_aluno_sistema' : 'Matrícula',
                       'modalidade_pagto' : 'Matrícula',
                       'situacao_matricula' : 'Matrícula',
                       'contrato_aprendizagem' : 'Matrícula',
                       'tipo_cota' : 'Matrícula',
                       'atestado_baixa_renda' : 'Matrícula',
                       'tipo_oferta' : 'Oferta',
                       'situacao_cpf' : 'Matrícula',
                       'numero_cpf' : 'Matrícula',
                       'subdependencia' : 'Instituição',
                       'carga' : '-',
                       'cod_municipio' : 'Instituição',
                       'modalidade_ensino' : 'Curso',
                       'dt_ocorrencia_ciclo' : 'Oferta',
                       'dt_ocorrencia_matricula' : 'Matrícula',
                       'data_ultima_alteracao' : 'Matrícula',
                       'vagas_ofertadas' : 'Oferta',
                       'total_inscritos' : 'Oferta',
                       'sg_etec' : 'Oferta',
                       'sg_proeja' : 'Oferta',
                       'sg_brasilpro' : 'Oferta',
                       'ano_convenio_brasilpro' : 'Oferta',
                       'status_ciclo_matricula' : 'Oferta',
                       'nome_completo_agrupador' : 'Instituição',
                       'sigla_agrupador' : 'Instituição',
                       'dt_data_finalizado' : 'Oferta',
                       'dt_validacao_conclusao' : 'Matrícula',
                       'data_geracao_codigo_diplomacertificado' : 'Matrícula (Diploma)',
                       'codigo_diploma_certificado' : 'Matrícula (Diploma)',
                       'nacionalidade' : 'Matrícula',
                       'co_aluno' : 'Matrícula',
                       'co_matricula' : 'Matrícula',
                       'st_deferimento_ue' : 'Instituição',
                       'st_ativo' : 'Instituição',
                       'co_inep' : 'Instituição',
                       'nu_cnpj' : 'Instituição',
                       'co_ies' : 'Instituição',
                       'co_endereco_e_mec' : 'Instituição',
                       'co_portfolio' : 'Curso',
                       'no_responsavel_emissao' : 'Matrícula' }

    return colunaCategoria[coluna]

def buscar_cod_coluna(coluna):
    """
        Função para buscar o código da coluna informada.

        Parameters
        ----------
        coluna : str
            Deve conter o nome da coluna que se deseja encontrar o código.

        Returns
        -------
        Int

        Examples
        --------
        >>> temp = buscar_cod_coluna ( 'municipio' )
    """

    cod_coluna = {'co_unidade_ensino' : 1,
                  'unidade_ensino' : 2,
                  'dt_deferimento_ue' : 3,
                  'dependencia_admin' : 4,
                  'sistema_ensino' : 5,
                  'municipio' : 6,
                  'estado' : 7,
                  'co_ciclo_matricula' : 8,
                  'dt_data_inicio' : 9,
                  'dt_data_fim_previsto' : 10,
                  'ds_eixo_tecnologico' : 11,
                  'co_curso' : 12,
                  'no_curso' : 13,
                  'dt_deferimento_curso' : 14,
                  'co_tipo_curso' : 15,
                  'tipo_curso' : 16,
                  'co_tipo_nivel' : 17,
                  'ds_tipo_nivel' : 18,
                  'experimental' : 19,
                  'nome_ciclo' : 20,
                  'dt_cadastro_ciclo' : 21,
                  'carga_horaria' : 22,
                  'programa_associado' : 23,
                  'nome_aluno' : 24,
                  'sg_sexo' : 25,
                  'dt_nascimento_aluno' : 26,
                  'dt_cadastro_aluno_sistema' : 27,
                  'modalidade_pagto' : 28,
                  'situacao_matricula' : 29,
                  'contrato_aprendizagem' : 30,
                  'tipo_cota' : 31,
                  'atestado_baixa_renda' : 32,
                  'tipo_oferta' : 33,
                  'situacao_cpf' : 34,
                  'numero_cpf' : 35,
                  'subdependencia' : 36,
                  'carga' : 37,
                  'cod_municipio' : 38,
                  'modalidade_ensino' : 39,
                  'dt_ocorrencia_ciclo' : 40,
                  'dt_ocorrencia_matricula' : 41,
                  'data_ultima_alteracao' : 42,
                  'vagas_ofertadas' : 43,
                  'total_inscritos' : 44,
                  'sg_etec' : 45,
                  'sg_proeja' : 46,
                  'sg_brasilpro' : 47,
                  'ano_convenio_brasilpro' : 48,
                  'status_ciclo_matricula' : 49,
                  'nome_completo_agrupador' : 50,
                  'sigla_agrupador' : 51,
                  'dt_data_finalizado' : 52,
                  'dt_validacao_conclusao' : 53,
                  'data_geracao_codigo_diplomacertificado' : 54,
                  'codigo_diploma_certificado' : 55,
                  'nacionalidade' : 56,
                  'co_aluno' : 57,
                  'co_matricula' : 58,
                  'st_deferimento_ue' : 59,
                  'st_ativo' : 60,
                  'co_inep' : 61,
                  'nu_cnpj' : 62,
                  'co_ies' : 63,
                  'co_endereco_e_mec' : 64,
                  'co_portfolio' : 65,
                  'no_responsavel_emissao' : 66 }

    return cod_coluna[coluna]

def verifica_boleano(valor_boleano):
    """
        Função para validar se o parâmetro informado é do tipo boleano ou não.

        Parameters
        ----------
        valor_boleano : boolean

        Returns
        -------
        Boolean

        Examples
        --------
        >>> temp = verifica_boleano ( valor_boleano )
    """

    lista_bool = 'sim não nao true false'
    valor_boleano = str(valor_boleano)

    # Verifica se o valor boleeano está correto
    if (valor_boleano.lower() in lista_bool):
        return True
    else:
        return False


def verifica_email(email):
    """
        Função para validar um email.

        Parameters
        ----------
        email : str
            Deve conter o email que se seja validar.

        Returns
        -------
        Boolean
            False se o email for invalido e True se o email for valido

        Examples
        --------
        >>> temp = valida_email(email)
    """

    REGEX = '^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$'

    if (re.search(REGEX, email)):
        return True
    else:
        return False


def verifica_cpf(cpf: str) -> bool:
    """
        Função para validar se o valor da linha que contém o cpf da coluna numero_cpf é valido.

        Parameters
        ----------
        cpf : str
            Deve conter a numeração do CPF que se deseja validar.

        Returns
        -------
        Boolean
            False se o cpf for invalido e True se o cpf for valido.

        Examples
        --------
        >>> temp = verifica_cpf(cpf)
    """

    TAMANHO_CPF = 11

    if len(cpf) != TAMANHO_CPF:
        return False

    if cpf in (c * TAMANHO_CPF for c in "1234567890"):
        return False

    cpf_reverso = cpf[::-1]
    for i in range(2, 0, -1):
        cpf_enumerado = enumerate(cpf_reverso[i:], start=2)
        dv_calculado = sum(map(lambda x: int(x[1]) * x[0], cpf_enumerado)) * 10 % 11
        if cpf_reverso[i - 1:i] != str(dv_calculado % 10):
            return False

    return True


def verifica_data(data):
    """
        Função para validar data não nula no formato dd/mm/aaaa.

        Parameters
        ----------
        data : str
            O parâmetro 'data' pode conter um valor vazio ou uma data no formato
            dd/mm/aaaa no tipo string

        Returns
        -------
        Boolean
            False se o data for invalido e True se o data for válido no formato.

        Examples
        --------
        >>> dt = verifica_data( '25/11/2023' )
    """

    formato = "%d/%m/%Y"
    res = True

    if (data == 'Nan' or data == 'nan' or data == 'None' or data == 0):
        return False
    try:
        res = bool(datetime.strptime(data, formato))
    except ValueError:
        res = False

    if (res == False):
        return False
    else:
        # faz o split e transforma em números
        dia, mes, ano = map(int, data.split('/'))

        # mês ou ano inválido (só considera do ano 1 em diante), retorna False
        if mes < 1 or mes > 12 or ano <= 0:
            return False

        # verifica qual o último dia do mês
        if mes in (1, 3, 5, 7, 8, 10, 12):
            ultimo_dia = 31
        elif mes == 2:
            # verifica se é ano bissexto
            if (ano % 4 == 0) and (ano % 100 != 0 or ano % 400 == 0):
                ultimo_dia = 29
            else:
                ultimo_dia = 28
        else:
            ultimo_dia = 30

        # verifica se o dia é válido
        if dia < 1 or dia > ultimo_dia:
            return False

    return True


def verifica_data_ano_valida(data):
    """
        Função para validar data não nula no formato 'aaaa' com 4 digitos.

        Parameters
        ----------
        data : str
            O parâmetro 'data' pode conter um valor vazio ou uma data no formato
            'aaaa' no tipo string

        Returns
        -------
        False ou True.
            False se o data for invalido e True se o data for válido no formato.

        Examples
        --------
        >>> dt = verifica_data_ano_valida(data)
    """
    lista = list(data)
    if (data != 'nan' or  data != 'null' or (type(data) != type(np.nan))):
        ano = int(data)

        # Verifica se o ano é negativo
        if ano <= 0:
            return False

        # Verifica se o ano é menor que 4 digitos
        elif len(lista) < 4 or len(lista) > 4:
            return False

        # verifica se é ano bissexto
        elif (ano % 4 == 0) and (ano % 100 != 0 or ano % 400 == 0):
            return True
    else:
        return True


def verifica_data_nulo(data):
    """
        Função para verificar se uma data passada está no formato dd/mm/aaaa

        Parameters
        ----------
        data : str
            O parâmetro 'data' pode conter um valor vazio ou uma data no formato
            dd/mm/aaaa no tipo string

        Returns
        -------
        Boolean
            False se o data for invalido e True se o data for válido no formato.

        Examples
        --------
        >>> temp = verifica_data_nulo(data)
    """

    # Verificação para as data que podem ter valores vazios
    formato = "%d/%m/%Y"
    res = True

    if (data == 'nan'):
        return True

    try:
        res = bool(datetime.strptime(data, formato))
    except ValueError:
        res = False

    if (res == False):
        return False

    # faz o split e transforma em números
    dia, mes, ano = map(int, data.split('/'))

    # mês ou ano inválido (só considera do ano 1 em diante), retorna False
    if mes < 1 or mes > 12 or ano <= 0:
        return False

    # verifica qual o último dia do mês
    if mes in (1, 3, 5, 7, 8, 10, 12):
        ultimo_dia = 31
    elif mes == 2:
        # verifica se é ano bissexto
        if (ano % 4 == 0) and (ano % 100 != 0 or ano % 400 == 0):
            ultimo_dia = 29
        else:
            ultimo_dia = 28
    else:
        ultimo_dia = 30

    # verifica se o dia é válido
    if dia < 1 or dia > ultimo_dia:
        return False

    return True


def verifica_data_valida(data):
    """
        Função para validar data no formato dd/mm/aaaa retornando um objeto
        do tipo Date a fim de facilitar as comparações com datas.

        Parameters
        ----------
        data : str
            Deve conter uma data no formato dd/mm/aaaa

        Returns
        -------
        Boolean
            False se o data for invalido e True se o data for valido

        Examples
        --------
        >>> temp = verifica_data(data)
    """

    formato = "%d/%m/%Y"
    res = True

    try:
        res = bool(datetime.strptime(data, formato))
    except ValueError:
        res = False

    if (res == False):
        return False
    else:
        data = datetime.strptime(data, "%d/%m/%Y").date()

    return data


# Função de auxilio para validar cnpj
def verifica_cnpj(cnpj: str) -> bool:
    TAMANHO_CNPJ = 14
    if len(cnpj) != TAMANHO_CNPJ:
        return False

    if cnpj in (c * TAMANHO_CNPJ for c in "1234567890"):
        return False

    cnpj_r = cnpj[::-1]
    for i in range(2, 0, -1):
        cnpj_enum = zip(cycle(range(2, 10)), cnpj_r[i:])
        dv = sum(map(lambda x: int(x[1]) * x[0], cnpj_enum)) * 10 % 11
        if cnpj_r[i - 1:i] != str(dv % 10):
            return False

    return True


# Função criada para auxiliar na validação da coluna geracao_codigo_diploma_certificado
def verifica_data_(data):
    formato = "%Y-%m-%d %H:%M:%S"
    res = True

    if (type(data) == type(np.nan)):
        return False
    try:
        res = bool(datetime.strptime(data, formato))
    except ValueError:
        res = False

    if (res == False):
        return False
    else:
        data, hora = map(str,data.split(' '))
        # faz o split e transforma em números
        ano, mes, dia = map(int, data.split('-'))

        # mês ou ano inválido (só considera do ano 1 em diante), retorna False
        if mes < 1 or mes > 12 or ano <= 0:
            return False

        # verifica qual o último dia do mês
        if mes in (1, 3, 5, 7, 8, 10, 12):
            ultimo_dia = 31
        elif mes == 2:
            # verifica se é ano bissexto
            if (ano % 4 == 0) and (ano % 100 != 0 or ano % 400 == 0):
                ultimo_dia = 29
            else:
                ultimo_dia = 28
        else:
            ultimo_dia = 30

        # verifica se o dia é válido
        if dia < 1 or dia > ultimo_dia:
            return False

    return True


###
# DEPRECATED
#
# Função criada para auxiliar na validação da coluna geracao_codigo_diploma_certificado
def verifica_hora(hora):
    formato = "%Y-%m-%d %H:%M:%S"
    res = True

    if (type(hora) == type(np.nan)):
        return False
    try:
        res = bool(datetime.strptime(hora, formato))
    except ValueError:
        res = False

    if (res == False):
        return False
    else:
        # faz o split e transforma em números
        data, hora = map(str,hora.split(' '))
        hora, minuto, segundo = map(int, hora.split(':'))

        # mês ou ano inválido (só considera do ano 1 em diante), retorna False
        if hora < 0  or hora >23:
            return False

        if minuto > 59 or minuto < 0:
            return False

        if segundo > 59 or segundo <0:
            return False

    return True


def data_hora_atual_como_string():
    """
        Função que extrai a data e hora do ssitema e retorna como string

        Parameters
        ----------
        None

        Returns
        -------
        str
            Retorna uma string no formato: %d_%m_%Y-%H_%M

        Examples
        --------
        >>> var = data_hora_atual_como_string()
    """
    data = datetime.now()
    data = data.strftime('%d_%m_%Y-%H_%M')

    return data

def gerar_csv_inconsistencia(df, nome_funcao, diretorio_e_nome_arquivo):
    """
        Função destinada a gerar arquivos de inconsistências

        Parameters
        ----------
        df : DataFrame
            Deve conter o DataFrame com os resultados a serem armazenados em csv.

        nome_funcao : str
            Deve conter o nome da função. pode-se adiconar o nome da coluna.

        diretorio_e_nome_arquivo : str
            Este parâmetro deve conter o caminho completo e nome do arquivo SEM
            a extensão .csv

        Returns
        -------
        None

        Examples
        --------
        >>> gerar_csv_inconsistencia(
                df_resultado,
                'valida_texto_nacionalidade',
                'd:/inconsistencias/base_1_valida_texto_nacionalidade_21_12_2022-20_05'
            )
    """

    if df.shape[0] == 0:
        print("\nNenhuma inconsistência foi encontrada ao processar [" + nome_funcao + "].\n")
    else:
        df.to_csv(diretorio_e_nome_arquivo + ".csv", index = False)
        print(" ■ Inconsistências geradas para: " + nome_funcao + "\n")


'''
    FIM: ↑  Funções que auxiliam nos processos  ↑
'''

