import sys
import os
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from src.class_hst2txt import HST2TXT
from src.class_indusoftData import Indusoft_data
from src.funcoes import export_lists_csv, gerar_lista_datas

from entradas import main_path, export_path,hst_gerador, hst2txt_exe_path, data_importacao

# Copy of input data from entradas.
main_path = main_path
export_path = export_path
hst_gerador = hst_gerador
hst2txt_exe_path = hst2txt_exe_path
#data_importacao = data_importacao # dd-mm-yyyy

print("############################################")
print("############ RELATORIO DIARIO ##############")
print("############################################")

#data_inicial = input("Entre a data de importação Inicial (Exemplo 01-01-2022): ")
data_inicial = '01-12-2023'
data_final = data_importacao

print("############################################")
print("############ INICIA RELATORIO ##############")
print("############################################")

# Extract Data from Source
Gerador = HST2TXT(hst2txt_exe_path, hst_gerador, 'Gerador_work')

pasta1 = Gerador.load_hst_file_to_work(data_inicial, data_final)

if pasta1:
    print('Ok')
else:
    print('Erro Pasta Vasia')
    #sys.exit()

Gerador.extract_hst_file_to_txt()
# Export path location to work with data
work_path = Gerador.work_path_location()
generator_path = Gerador.new_data_path()

Dados_Gerador = Indusoft_data(work_path, generator_path)
Dados_Gerador.set_labels(["K1Q1_PAP","K2Q1_PAP","K3Q1_PAP","K4Q1_PAP","K5Q1_PAP","K6Q1_PAP","K7Q1_PAP","K8Q1_PAP","K9Q1_PAP"])

lista_data = gerar_lista_datas(data_inicial, data_final)

for data_importacao in lista_data:

    if Dados_Gerador.verifica_arquivos(data_importacao):
        print(f'Arquivos do dia {data_importacao} existentes!')
    else:
        continue

    Maximos_do_dia = Dados_Gerador.get_database_max(data_importacao, False)

    export_lists_csv(data_importacao, Maximos_do_dia, export_path=export_path)

print("############################################")
print("############ RELATORIO PRONTO ##############")
print("############################################")

Gerador.remove_work()