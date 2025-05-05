
from IPython.display import clear_output
clear_output()


from exploracao_dados_sistec_lib_otimizado_v4 import *

caminho_completo_base = "C:/Users/niniv/OneDrive/Área de Trabalho/Trabalho-de-Conclusao-de-Curso/"

"""
    Arquivo criado para processar a base versão 4 da base de dados fornecdida.
    Para isso, é necessário compreender e configurar as variáveis essenciais:

    DIR_INCONSISTENCIAS: caminho da pasta que receberá os arquivos
                           de inconsistências gerados.

    bases: deve conter a flag 'processar_base' (True, False) e o 'nome'
             da base de dados.

    qtdRegistrosParaProcessar: Deve conter um inteiro sinalizando a quantidade
                                 de dados a processar ou None para processar doso os dados.

    desejoProcesarDuplicidades: configurar com True ou False para processar duplicidades.

    desejoExtrairAmostras: configurar com True ou False para exportar uma certa
                             quantidade de registros definida pela variáveis
                             'qtdRegistrosAmostra'.

    qtdRegistrosAmostra: valor inteiro que define a quantidade de registros se
                           deseja extrair da base.

    listaColunas: definir uma lista de colunas que se deseja trabalhar ou a palavra
                    'None' caso se deseja usar todas as colunas da base.

    novosNomesColunas: contém uma lista com os nomes das colunas que se deseja
                         definir para trabalhar.

    processar_funcao: variável usada para ativar e desativar as funções que
                        se deseja processar ao longo do código.

    mapa_funcoes: variável dicionário usada para configurar e mapear o processo
                    de execução de casa função, sendo que, seu índice principal
                    é o nome da função. Esse indice recebe 3 outras variáveis:
                        'funcao': que recebe o objeto que contém o corpo da função;
                        'status': recebe a variável processar_função;
                        'param': que deve conter a lista de colunas a processar.

"""

import glob
import sys
import pandas as pd
import os
import psutil

from datetime import datetime

# processar_base: se True, processa a base, senão, não processa
bases = {
    'base_dados_1' : {'processar_base' : True, 'nome': 'sistec_ifb.csv'},
    'base_dados_2' : {'processar_base' : False, 'nome': ''}
}

qtdRegistrosParaProcessar = None
desejoProcesarDuplicidades = False
listaColunas = None

# ajuste dos nomes das colunas da base -> versão IFB SISTEC
novosNomesColunas = [
    "Aluno",
    "co_ciclo_matricula",
    "unidade_ensino",
    "dependencia_adm",
    "sistema_ensino",
    "municipio",
    "estado",
    "periodo",
    "dt_data_inicio",
    "dt_data_fim_previsto",
    "ds_eixo_tecnologico",
    "co_curso",
    "no_curso",
    "dt_deferimento_curso",
    "co_tipo_curso",
    "tipo_curso",
    "co_tipo_nivel",
    "ds_tipo_nivel",
    "nome_ciclo",
    "dt_cadastro_ciclo",
    "carga_horaria",
    "dt_cadastro_aluno_sistema",
    "periodo_cadastro_matricula_ano",
    "modalidade_pagto",
    "situacao_matricula",
    "tipo_cota",
    "atestado_baixarenda",
    "tipo_oferta",
    "ano",
    "modalidade_ensino",
    "dt_ocorrencia_ciclo",
    "dt_ocorrencia_matricula",
    "data_ultima_alteracao",
    "vagas_ofertadas",
    "total_inscritos",
    "co_status_matricula",
    "sg_sexo",
    "dt_data_nascimento",
    "nome_completo_agrupador",
    "sigla_agrupador"
]

# habilitar o uso de determinadas funções
processar_funcao = {
    'valida_inteiro_nao_nulo_status' : True,
    'valida_ds_eixo_tecnologico_status' :True,
    'valida_inteiro_status' : True,
    'valida_texto_nao_nulo_status' : True,
    'valida_estado_status' : True,
    'valida_sexo_aluno_status' : True,
    'valida_texto_status' : True,
    'valida_bool_nao_nulo_status' : True,
    'valida_bool_status' : True,
    'valida_tipo_cota_status' : True,
    'valida_sg_brasilpro_status' : False,
    'valida_numero_cpf_status' : False,
    'valida_subdependencia_status' : False, #
    'valida_tipo_oferta_status' : True,
    'valida_dt_deferimento_curso_status' : False,
    'valida_dt_cadastro_aluno_sistema_status' : True,
    'valida_dt_cadastro_ciclo_status': True,
    'valida_dt_data_fim_previsto_status' : True,
    'valida_data_nao_nulo_status' : False,
    'valida_data_status' : False,
    'valida_ano_convenio_brasil_pro_status' : False,
    'valida_dt_validacao_conclusao_status' : False,
    'valida_nu_cnpj_status' : True,
    'valida_co_endereco_co_ies_status' : False,
    'valida_no_responsavel_emissao_status' : False, #
    'valida_diploma_certificado_status' : False #
}

# mapeamento da funções em dicionário para automatização do processo de execução
mapa_funcoes = {
    'valida_inteiro_nao_nulo': {'funcao': valida_inteiro_nao_nulo,
                                'status': processar_funcao['valida_inteiro_nao_nulo_status'],
                                'param': ['codigo_universal_linha',
                                          #'co_unidade_ensino',
                                          'co_ciclo_matricula',
                                          'co_curso',
                                          'co_tipo_curso',
                                          'co_tipo_nivel',
                                          #'carga_horaria',
                                          #'co_aluno',
                                          #'co_portfolio',
                                          #'#co_matricula'
                                          ]},

    'valida_ds_eixo_tecnologico': {'funcao': valida_ds_eixo_tecnologico,
                                   'status': processar_funcao['valida_ds_eixo_tecnologico_status'],
                                   'param': ['codigo_universal_linha',
                                             'co_tipo_curso',
                                             'ds_eixo_tecnologico']},

    'valida_inteiro': {'funcao': valida_inteiro,
                       'status': processar_funcao['valida_inteiro_status'],
                       'param': ['codigo_universal_linha',
                                 #'cod_municipio',
                                 'vagas_ofertadas',
                                 'total_inscritos',
                                 #co_inep'
                                 ]},

    'valida_texto_nao_nulo': {'funcao': valida_texto_nao_nulo,
                              'status': processar_funcao['valida_texto_nao_nulo_status'],
                              'param': ['codigo_universal_linha',
                                        'unidade_ensino',
                                        #'dependencia_admin',
                                        'sistema_ensino',
                                        'municipio',
                                        'no_curso',
                                        'tipo_curso',
                                        'ds_tipo_nivel',
                                        'nome_ciclo',
                                        #'nome_aluno',
                                        'modalidade_pagto',
                                        'situacao_matricula',
                                        'modalidade_ensino',
                                        #'status_ciclo_matricula',
                                        'sigla_agrupador',
                                        'nome_completo_agrupador'
                                        ]},

    'valida_estado': {'funcao': valida_estado,
                      'status': processar_funcao['valida_estado_status'],
                      'param': ['codigo_universal_linha',
                                'estado']},

    'valida_sexo_aluno': {'funcao': valida_sexo_aluno,
                          'status': processar_funcao['valida_sexo_aluno_status'],
                          'param': ['codigo_universal_linha',
                                    'sg_sexo']},

    'valida_texto': {'funcao': valida_texto,
                     'status': processar_funcao['valida_texto_status'],
                     'param': ['codigo_universal_linha',
                               #'nacionalidade'
                               ]},

    'valida_bool_nao_nulo': {'funcao': valida_bool_nao_nulo,
                             'status': processar_funcao['valida_bool_nao_nulo_status'],
                             'param': ['codigo_universal_linha',
                                       #'experimental',
                                       #'contrato_aprendizagem',
                                       #'sg_etec',
                                       #'st_ativo',
                                       #'sg_brasilpro'
                                       ]},

    'valida_bool': {'funcao': valida_bool,
                    'status': processar_funcao['valida_bool_status'],
                    'param': ['codigo_universal_linha',
                              #'carga',
                              #'sg_proeja'
                              ]},

    'valida_tipo_cota': {'funcao': valida_tipo_cota,
                         'status': processar_funcao['valida_tipo_cota_status'],
                         'param': ['codigo_universal_linha',
                                   'tipo_cota']},

    'valida_sg_brasilpro': {'funcao': valida_sg_brasilpro,
                            'status': processar_funcao['valida_sg_brasilpro_status'],
                            'param': ['codigo_universal_linha',
                                      #'sg_brasilpro',
                                      #'ano_convenio_brasilpro'
                                      ]},

    'valida_no_responsavel_emissao': {'funcao': valida_no_responsavel_emissao,
                          'status': processar_funcao['valida_no_responsavel_emissao_status'],
                          'param': ['codigo_universal_linha',
                                    'no_responsavel_emissao',
                                    'codigo_diploma_certificado']},

    'valida_numero_cpf': {'funcao': valida_numero_cpf,
                          'status': processar_funcao['valida_numero_cpf_status'],
                          'param': ['codigo_universal_linha',
                                    'situacao_cpf',
                                    'numero_cpf']},

    'valida_subdependencia': {'funcao': valida_subdependencia,
                          'status': processar_funcao['valida_subdependencia_status'],
                          'param': ['codigo_universal_linha',
                                    'dependencia_admin',
                                    'subdependencia']},

    'valida_tipo_oferta': {'funcao': valida_tipo_oferta,
                           'status': processar_funcao['valida_tipo_oferta_status'],
                           'param': ['codigo_universal_linha',
                                     'tipo_oferta',
                                     'co_tipo_curso']},

    'valida_diploma_certificado': {'funcao': valida_diploma_certificado,
                                          'status': processar_funcao['valida_diploma_certificado_status'],
                                          'param': ['codigo_universal_linha',
                                                    'codigo_diploma_certificado',
                                                    'data_geracao_codigo_diplomacertificado',
                                                    'situacao_matricula',
                                                    'co_tipo_curso']},

    'valida_dt_deferimento_curso': {'funcao': valida_dt_deferimento_curso,
                                    'status': processar_funcao['valida_dt_deferimento_curso_status'],
                                    'param': ['codigo_universal_linha',
                                              'dt_deferimento_ue',
                                              'dt_deferimento_curso']},

    'valida_dt_cadastro_aluno_sistema': {'funcao': valida_dt_cadastro_aluno_sistema,
                                         'status': processar_funcao['valida_dt_cadastro_aluno_sistema_status'],
                                         'param': ['codigo_universal_linha',
                                                   'dt_cadastro_aluno_sistema',
                                                   'dt_cadastro_ciclo']},

    'valida_dt_cadastro_ciclo': {'funcao': valida_dt_cadastro_ciclo,
                                         'status': processar_funcao['valida_dt_cadastro_ciclo_status'],
                                         'param': ['codigo_universal_linha',
                                                   'dt_cadastro_ciclo',
                                                   'dt_deferimento_curso']},

    'valida_dt_data_fim_previsto': {'funcao': valida_dt_data_fim_previsto,
                                    'status': processar_funcao['valida_dt_data_fim_previsto_status'],
                                    'param': ['codigo_universal_linha',
                                              'co_tipo_curso',
                                              'dt_data_inicio',
                                              'dt_data_fim_previsto']},

    'valida_data_nao_nulo': {'funcao': valida_data_nao_nulo,
                             'status': processar_funcao['valida_data_nao_nulo_status'],
                             'param': ['codigo_universal_linha',
                                       'dt_data_inicio',
                                       'dt_cadastro_ciclo',
                                       #'dt_nascimento_aluno',
                                       'dt_ocorrencia_ciclo',
                                       'dt_ocorrencia_matricula',
                                       'data_ultima_alteracao',
                                       #'dt_deferimento_ue'
                                       ]},

    'valida_data': {'funcao': valida_data,
                    'status': processar_funcao['valida_data_status'],
                    'param': ['codigo_universal_linha',
                              'dt_data_finalizado']},

    'valida_ano_convenio_brasil_pro': {'funcao': valida_ano_convenio_brasil_pro,
                                       'status': processar_funcao['valida_ano_convenio_brasil_pro_status'],
                                       'param': ['codigo_universal_linha',
                                                 #'ano_convenio_brasilpro'
                                                 ]},

    'valida_dt_validacao_conclusao': {'funcao': valida_dt_validacao_conclusao,
                                      'status': processar_funcao['valida_dt_validacao_conclusao_status'],
                                      'param': ['codigo_universal_linha',
                                                'dt_validacao_conclusao',
                                                'data_geracao_codigo_diplomacertificado']},

    'valida_nu_cnpj': {'funcao': valida_nu_cnpj,
                       'status': processar_funcao['valida_nu_cnpj_status'],
                       'param': ['codigo_universal_linha',
                                 #'nu_cnpj'
                                 ]},

    'valida_co_endereco_co_ies': {'funcao': valida_co_endereco_co_ies,
                                  'status': processar_funcao['valida_co_endereco_co_ies_status'],
                                  'param': ['codigo_universal_linha',
                                            'co_ies',
                                            'co_endereco_e_mec',
                                            'sistema_ensino',
                                            'dependencia_admin']}

}


for chave_base, base_dados in bases.items():

    if base_dados['processar_base']:
        
        nome_base = base_dados['nome']
        
        inicioTempo = time.time()

        print("\n>> Processando a base " + nome_base + " <<\n")

        arquivo_dados = caminho_completo_base + nome_base

        if desejoProcesarDuplicidades:
            listaColunas = ['co_aluno', 'co_matricula']
            qtdRegistrosParaProcessar = None
        else:
            listaColunas = None

        df_base_dados_chunk = pd.read_csv( 
            arquivo_dados, 
            keep_default_na = False,
            #engine = 'python',
            header = 0,
            on_bad_lines = 'skip', 
            low_memory = False, 
            quotechar = '"',
            quoting = 0,
            skiprows = 0,
            usecols = listaColunas,
            nrows = qtdRegistrosParaProcessar, 
            chunksize = 500000
        )
        
        print("Processando chunks...")
        chunks = []
        c = 1
        for chunk in df_base_dados_chunk:
            chunks.append(chunk)
            print(f"{c}: {chunk.shape}")
            c = c + 1

        print("\nConcatenando os chunks...")
        df_base_dados = pd.concat(chunks)
        
        print("\nApagando variáveis desnecessárias...")
        del chunks, chunk, c, df_base_dados_chunk
        
        print('\nMemória RAM usada: ' + str(psutil.virtual_memory()[2]) + ' %') 
        print('Quantidade de memória RAM usada até aqui (GB): ' + str(psutil.virtual_memory()[3] / 1000000000))
        
        if not desejoProcesarDuplicidades:
            # Renomeação das colunas
            print("\Colunas NÃO ajustadas")
            print(df_base_dados.columns)

            df_base_dados = df_base_dados.replace({'\n' : '', "null" : '', r'\A\s+|\s+\Z' : np.nan}, regex = True)
            df_base_dados = df_base_dados.replace('', np.nan)
            print("Espaços vazios foram preenchidos com " + str(np.nan))

            consumo_memo = str(round(sys.getsizeof(df_base_dados) / 1048576, 3)) + " MB"
            print("Consumo de memória do dataframe geral: ", consumo_memo)

            # Adicionando uma coluna com código universal
            df_base_dados['codigo_universal_linha'] = df_base_dados['Aluno'].astype(str) + '_' + df_base_dados['co_ciclo_matricula'].astype(str)
            print("Coluna de código universal adicionada.")

        print("\nResumo dos dados: ")
        print("> Quantidade de colunas encontradas .........: " + str(df_base_dados.shape[1]))
        print("> Quantidade total de registros processados .: " + str(df_base_dados.shape[0]))
        print('> Quantidade de RAM usada (GB) ..............: ' + str(psutil.virtual_memory()[3] / 1000000000))

        fimTempo = time.time()
        print("> Tempo gasto na configuração ...............: " + str(round(float(fimTempo - inicioTempo), 3)) + " seg.")


        if desejoProcesarDuplicidades:
            print("\n>> Processando duplicidades...")

            res = buscar_dados_repetidos(df_base_dados)

            if res.shape[0] == 0:
                print("> Nenhuma duplicidade foi encontrada nos registros processados.")
            else:
                data = data_hora_atual_como_string()
                path = DIR_INCONSISTENCIAS + "/duplicidades_" + str(chave_base) + '_' + str(data) + "_" + str(nome_base)
                res.to_csv(path, index = False)
                print("> Inconsistências geradas no arquivo: " + path)
                del data, path, res


        if not desejoProcesarDuplicidades:
            for nome_funcao, mapa_funcao in mapa_funcoes.items():
                if mapa_funcao['status']:
                    print("\n>----->>>")
                    print(">> Processando a função " + nome_funcao)
                    print("> Colunas usadas: " + str(mapa_funcao["param"]) + "\n")
                    
                    df_colunas = df_base_dados[mapa_funcao["param"]]
                    base_info = {'id': chave_base, 'nome': nome_base}
                    mapa_funcao['funcao'](df_colunas, base_info)
                    
                    print("<<<-----<")

            # limpa memória
            del nome_funcao, mapa_funcao, df_colunas, base_info


        # Ajustes e contabilização do tempo gasto
        df_tempo_gasto_func = pd.DataFrame(columns = ["funcao", "tempo_gasto"])
        soma_tempo_gasto = 0

        # for i, v in global_tempo_gasto_funcoes.items():
        #     df_aux = {'funcao': i, "tempo_gasto": v}
        #     soma_tempo_gasto = soma_tempo_gasto + float(v)
        #     df_tempo_gasto_func = df_tempo_gasto_func.append(df_aux, ignore_index = True)

        # df_aux = {'funcao': 'TOTAL DE TEMPO GASTO', "tempo_gasto": soma_tempo_gasto}
        # df_tempo_gasto_func = df_tempo_gasto_func.append(df_aux, ignore_index = True)
        # df_tempo_gasto_func.to_csv(DIR_INCONSISTENCIAS + "/" + str(chave_base) + "_tempo_gasto_func.csv", index = False)

        # print(df_tempo_gasto_func)
        # del df_tempo_gasto_func, soma_tempo_gasto, i, v, df_aux

print("\n\n>>>>>> Script finalizado com sucesso <<<<<<< ")

