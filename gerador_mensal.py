######################################################################
# Titulo: Gerador de relatório
# Autor: Afonso Roberto Plantes Neto
# Linguagem: Python
# Setor: Casa de Força
# Descrição: Gera o relatório de consumo e geração mensal
######################################################################
from entradas import main_path, export_path, hst_gerador, hst_caldeira, hst2txt_exe_path, data_importacao

data_inicial = '01-12-2023'
data_final = data_importacao

from src.class_hst2txt import HST2TXT

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import warnings
warnings.filterwarnings('ignore')

# Pacote de funções Customizadas
from src.hst_process_lib import verificar_arquivo,get_database,gera_lista_media_por_hora,lista_de_dias,gera_relatorio_dias_mes,exporta_dados_diario,gerar_lista_datas

label_potencia_real= ["K1Q1_PA","K2Q1_PA","K3Q1_PA","K4Q1_PA","K5Q1_PA","K6Q1_PA","K7Q1_PA","K8Q1_PA","K9Q1_PA"]
label_potencia_reativa= ["K1Q1_PR","K2Q1_PR","K3Q1_PR","K4Q1_PR","K5Q1_PR","K6Q1_PR","K7Q1_PR","K8Q1_PR","K9Q1_PR"]
label_potencia_aparente= ["K1Q1_PAP","K2Q1_PAP","K3Q1_PAP","K4Q1_PAP","K5Q1_PAP","K6Q1_PAP","K7Q1_PAP","K8Q1_PAP","K9Q1_PAP"]

lista_cabecalhos_export = ["Diesel",
                           "Turbogerador",
                           "Concessionaria",
                           "Caldeira",
                           "ETE",
                           "Fermentacao",
                           "Secador",
                           "Destilaria",
                           "Industria",
                           'Data',
                           'Hora',
                           'Unidade']

########################################################
######### 

def acrescentar_kw(lista_strings,str_adicionar):
    nova_lista = [s + str_adicionar for s in lista_strings]
    return nova_lista

def gera_relatorio_gerador(data_escolhida,label_dados_gerador,in_lista_cabecalhos,in_unidade,transpose_data,main_path):

    ano = data_escolhida[-2:]
    mes = data_escolhida[3:5]
    dia = data_escolhida[0:2]
    
    file_name = "01" + ano + mes + dia

    pasta_do_gerador = os.path.join(main_path, 'Gerador_work')

    df_gerador = get_database(pasta_do_gerador, file_name)
    
    points_per_hour = 90


    df_gerador['Data_Hora'] = df_gerador['Data_Hora'].dt.hour
    df_sum = df_gerador.groupby('Data_Hora').sum()
    df = df_sum[label_dados_gerador]/points_per_hour 

    df['Data'] = pd.DataFrame([data_escolhida ]* 24)
    df['Hora'] = pd.DataFrame(list(range(24)))    
    df['Unidade'] = pd.DataFrame([in_unidade] * 24)   

    df.columns = in_lista_cabecalhos
    
    if transpose_data:
        df = df.transpose()
    return df

######### 
########################################################

################################################
# Escopo de Execução do script

# Localização da base de dados do supervisório
arquivos_gerador =  hst_gerador
arquivos_caldeira =  hst_caldeira
hst2txt_exe_path = hst2txt_exe_path

main_path = main_path
export_path = export_path

transpose_data = False

Gerador = HST2TXT(hst2txt_exe_path, hst_gerador, 'Gerador_work')
Gerador.load_hst_file_to_work(data_inicial, data_final)
Gerador.extract_hst_file_to_txt()


lista_datas = gerar_lista_datas(data_inicial,data_final)

df_potencia_ativa = pd.DataFrame(columns=lista_cabecalhos_export)
df_potencia_reativa = pd.DataFrame(columns=lista_cabecalhos_export)
df_potencia_aparente = pd.DataFrame(columns=lista_cabecalhos_export)


main_path = Gerador.work_path_location()


for data_nova in lista_datas:
    #print("Relatorio do dia: {}".format(data_nova))
    
    if verificar_arquivo(data_nova, main_path, 'Gerador_work'):
        
        df_dados = gera_relatorio_gerador(data_nova,label_potencia_real,lista_cabecalhos_export,'kwh',transpose_data,main_path)
        df_potencia_ativa = df_potencia_ativa.append(df_dados, ignore_index=True)
        
        df_dados = gera_relatorio_gerador(data_nova,label_potencia_reativa,lista_cabecalhos_export,'kvarh',transpose_data,main_path)
        df_potencia_reativa = df_potencia_reativa.append(df_dados, ignore_index=True)

        df_dados = gera_relatorio_gerador(data_nova,label_potencia_aparente,lista_cabecalhos_export,'kvah',transpose_data,main_path)
        df_potencia_aparente = df_potencia_aparente.append(df_dados, ignore_index=True)

df_potencias = pd.DataFrame(columns=lista_cabecalhos_export)

df_potencias = df_potencias.append(df_potencia_ativa, ignore_index=True)
df_potencias = df_potencias.append(df_potencia_reativa, ignore_index=True)
df_potencias = df_potencias.append(df_potencia_aparente, ignore_index=True)

#export_path = 'C:\\Users\\afons\\Desktop\\Github\\dados_safras\\cogeracao\\relatorios\\safra{}\\'.format(data_final[-4:])

export_path = os.path.join(export_path,'safra{}'.format(data_final[-4:]))

try:
    os.mkdir(export_path)
except:
    print('Pasta {} existente'.format(export_path))

export_path = os.path.join(export_path,'dados_potencias.csv')

print(export_path)

df_potencias.applymap(lambda x: str(x).replace('.', ',')).to_csv(export_path, sep='\t', index=False)

Gerador.remove_work()
print('Relatório Pronto')
