import glob
import os 
import pandas as pd 
import numpy as np 
import os.path

diretorio = 'inconsistencias'
def verificar_duplicidade(df_sistec):
    colunas = [
        'Aluno', 'co_ciclo_matricula', 'dt_data_inicio', 'dt_data_fim_previsto',
        'no_curso', 'nome_ciclo', 'situacao_matricula', 'periodo_cadastro_matricula_aluno'
    ]
    duplicados = df_sistec[df_sistec.duplicated(subset=colunas, keep=False)]
    duplicados = duplicados.sort_values(by='Aluno')
    duplicados.to_csv(f'{diretorio }/sistec_duplicados.csv', sep=',', index=False, encoding='utf-8-sig')

# Deixar essa em aberto, conversar na reunião.
def verificar_duplicados_info_diferente(df_sistec):
    alunos_repetidos = {}
    diferente = []
    #Transformando a data em data padrao para todos 
    df_sistec['dt_data_nascimento'] = pd.to_datetime(df_sistec['dt_data_nascimento'], errors='coerce', dayfirst=True)
    duplicados = df_sistec[df_sistec.duplicated(subset=['Aluno'], keep=False)]
    duplicados = duplicados.sort_values(by='Aluno')
    for index, linha in duplicados.iterrows():
          aluno = linha['Aluno']
          sexo_aluno = linha['sg_sexo']
          dt_nascimento = linha['dt_data_nascimento']
          if aluno not in alunos_repetidos:
             alunos_repetidos[aluno] = (sexo_aluno, dt_nascimento)
          else:
           sexo_aluno_verifica, dt_nascimento_verifica = alunos_repetidos[aluno]
           if sexo_aluno != sexo_aluno_verifica or dt_nascimento  != dt_nascimento_verifica:
              diferente.append(index)
    
    df_sistec_diferente = df_sistec.loc[diferente]
    df_sistec_diferente.to_csv(f'{diretorio }/sistec_duplicados_infor_diferente.csv', sep=',', index=False, encoding='utf-8-sig')
    print(df_sistec_diferente)

def unir_arquivos():
    arquivos_csv = glob.glob(os.path.join(diretorio+ "/sistec_ifb*.csv")) 
    print(arquivos_csv)
    base_dados = pd.DataFrame()
    for arquivo in arquivos_csv:
        df_temp = pd.read_csv(arquivo)
        base_dados  = pd.concat([df_temp,base_dados])
    df_dados_ = base_dados.replace(np.nan, 'vazio')
    df_dados_.to_csv(f'{diretorio }/sistec_ifb_inconsistencias_unido.csv', index=False)
    print("CSV com todas as inconsistências criado")

# if __name__ == '__main__':
#    df_sistec = pd.read_csv('sistec_ifb.csv', sep=';',encoding='utf-8')
#    verificar_duplicados_info_diferente(df_sistec)