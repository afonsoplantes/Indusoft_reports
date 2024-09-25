
########################################################
######### Escopo de bibliotecas 

import os
from tqdm import tqdm
import pandas as pd
import shutil
import numpy as np
from datetime import datetime, timedelta


######### Fim Pacote de Funções Extração Arquivos .HST
########################################################


########################################################
######### Remove Arquivos de uma extensão especifica
########################################################
def remove_file_tree(file_path,file_extention):

    file_list = [os.path.join(file_path,f) for f in os.listdir(file_path) if f.endswith(file_extention)]
    for f in file_list:
        print(f)
        os.remove(f)
######### Fim Remove Arquivos de uma extensão especifica
########################################################

########################################################
######### GEra uma lista de dias
########################################################
def lista_de_dias(a,b):
  lista_dias=list()
  for i in range(a,a+b):
    if i<10:
      lista_dias.append('0'+str(i))
    else:
      lista_dias.append(str(i))
  return lista_dias
 ######### Fim 
########################################################

########################################################
######### Exporta um lista com a media por hora de um valor
def gera_lista_media_por_hora(dados_entrada):
    df_item = dados_entrada

    lista_item = list()
    soma_item = 0   

    count = 0
    for k in range(len(df_item)):
        soma_item += df_item[k]
        count+=1
        if count > 90:
            lista_item.append(soma_item/90)
            count = 0
            soma_item = 0
    return lista_item
######### 
########################################################

########################################################
######### Obtem dados Brutos do Supervisório

# Redução de uma lista
def flatten(t):
    return [item for sublist in t for item in sublist]
# Leitura do Banco de dados
def get_database(data_path: str, file_name: str):

  ano = file_name[2:-4]
  mes = file_name[4:-2]
  dia = file_name[6:]
  # Montagem do caminho dos arquivos

  file_path = os.path.join(os.path.join(data_path,'TXT_FILES'), file_name + '.txt')
  header_path = os.path.join(os.path.join(data_path,'HDR_FILES'), file_name + '.hdr')

  print(file_path)
  print(header_path)
  # Leitura do arquivo de cabeçalho
  header_list = []
  ## Remove impuresa do arquivo de header
  cabecas = pd.read_csv(header_path, names = ['cabecas'])
  cabecas['cabecas'] = cabecas['cabecas'].str.strip()
  cabecas['cabecas'] = cabecas['cabecas'].str.replace('.','_')
  lista = cabecas[1:].values.tolist()
  header_list = flatten(lista)
  header_list.insert(0,'Hora')
  header_list.insert(0,'Dia')
  # Cria dataframe com header ajustado
  dados_brutos = pd.read_csv(file_path, sep='\t', names = header_list)
  return dados_brutos

def get_database(data_path: str, file_name: str):
        
    # Join File Path
    file_path = os.path.join(os.path.join(data_path,'TXT_FILES'), file_name + '.txt')
    header_path = os.path.join(os.path.join(data_path,'HDR_FILES'), file_name + '.hdr')

    # Read header File
    header_list = []
    ## Purge dirty data from header file
    cabecas = pd.read_csv(header_path, names = ['cabecas'])
    cabecas['cabecas'] = cabecas['cabecas'].str.strip()
    cabecas['cabecas'] = cabecas['cabecas'].str.replace('.','_')
    lista = cabecas[1:].values.tolist()
    header_list = flatten(lista)
    header_list.insert(0,'Hora')
    header_list.insert(0,'Data')
    dados_brutos = pd.read_csv(file_path, sep='\t', names = header_list)

    dados_brutos['Data'] = pd.to_datetime(dados_brutos['Data'], format='%d/%m/%Y')
    dados_brutos['Data_Hora'] = dados_brutos['Data'] + pd.to_timedelta(dados_brutos['Hora'])
    primeiro_item = dados_brutos['Data_Hora'].iloc[0]
    if 23 <= primeiro_item.hour < 24:
        dados_brutos['Data_Hora'] = dados_brutos['Data_Hora'] + pd.DateOffset(hours=1)
    # Remove as colunas 'Data' e 'Hora'.
    dados_brutos = dados_brutos.drop(['Data', 'Hora'], axis=1)
    # Obtém a ordem atual das colunas.
    colunas = dados_brutos.columns.tolist()
    # Move a coluna 'Data_Hora' para a primeira posição.
    colunas = ['Data_Hora'] + [coluna for coluna in colunas if coluna != 'Data_Hora']
    dados_brutos = dados_brutos[colunas]
    return dados_brutos
######### 
########################################################


########################################################
######### 
def gera_relatorio_dias_mes(data_escolhida,lista_cabecalhos_export,
                            label_dados_gerador,label_dados_caldeira,
                            transpose_data,main_path):

    ano = data_escolhida[-2:]
    mes = data_escolhida[3:5]
    dia = data_escolhida[0:2]
    
    file_name = "01" + ano + mes + dia

    pasta_do_gerador = main_path + 'Gerador_work\\'
    pasta_da_caldeira = main_path + 'Caldeira_work\\'  

    df_gerador = get_database(pasta_do_gerador, file_name)
    lista_dados = list()
    for jj in range(len(label_dados_gerador)):
        lista_dados.append(gera_lista_media_por_hora(df_gerador[label_dados_gerador[jj]]))
    
    df_caldeira = get_database(pasta_da_caldeira, file_name)
    for jj in range(len(label_dados_caldeira)):
        lista_dados.append(gera_lista_media_por_hora(df_caldeira[label_dados_caldeira[jj]]))
    
    df = pd.DataFrame(lista_dados)
    #print(df)
    df_dados = df.transpose()
    df_dados.columns = lista_cabecalhos_export
    if transpose_data:
        df_dados = df_dados.transpose()
        
    return df_dados
######### 
########################################################


########################################################
######### 
def exporta_dados_diario(df_dados,data_escolhida,main_path):
    
    ano = data_escolhida[-2:]
    mes = data_escolhida[3:5]
    dia = data_escolhida[0:2]

    export_path = main_path + 'safras20'+ ano + '\\' + "relatorio_diario" 
    
    try:
       os.mkdir(export_path)
    except:
       print(f"Caminho de Exportação: {export_path}")

    try:
       os.mkdir(export_path + '\\' + mes)
    except:
       print(f'Data de Exportação: {data_escolhida}')

    with pd.ExcelWriter(export_path + '\\' + mes + "/20" + ano + "-" + mes + "-" + dia + ".xlsx") as writer:
       df_dados.to_excel(writer, sheet_name="dados_cogeracao")
######### 
########################################################

########################################################
######### 
from datetime import datetime, timedelta

def gerar_lista_datas(data_inicial_str: str, data_final_str: str):
    data_inicial = datetime.strptime(data_inicial_str, '%d-%m-%Y')
    data_final = datetime.strptime(data_final_str, '%d-%m-%Y')

    lista_datas = []

    while data_inicial <= data_final:
        lista_datas.append(data_inicial.strftime('%d-%m-%Y'))
        data_inicial += timedelta(days=1)

    return lista_datas

######### 
########################################################


def verificar_arquivo(data_escolhida,caminho_arquivo,pasta):
    
    ano = data_escolhida[-2:]
    mes = data_escolhida[3:5]
    dia = data_escolhida[0:2]
    
    file_name = "01" + ano + mes + dia

    pasta_do_gerador = os.path.join(caminho_arquivo,pasta)

    print(pasta_do_gerador)
    
    header_path = os.path.join(pasta_do_gerador, 'HDR_FILES')
    header_path = os.path.join(header_path, file_name + '.hdr')

    print(header_path)
    if os.path.exists(header_path):
        return True
    else:
        print("O arquivo do dia {} não existe.".format(data_escolhida))
        return  False