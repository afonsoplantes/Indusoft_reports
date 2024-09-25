import sys
import os
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')

from src.class_hst2txt import HST2TXT
from src.class_indusoftData import Indusoft_data
from src.funcoes import exporta_dados_diario

from entradas import main_path, export_path, hst_gerador, hst_destilaria, hst2txt_exe_path, data_importacao

# Copy of input data from entradas.
main_path = main_path
export_path = export_path
hst_data = hst_destilaria
hst2txt_exe_path = hst2txt_exe_path

print("############################################")
print("############ RELATORIO DIARIO ##############")
print("############################################")

#data_importacao = data_importacao # dd-mm-yyyy
#data_importacao = input("Entre a data de importação (Exemplo 01-01-2022): ")
data_importacao = '25-06-2024'
print("############################################")
print("############ INICIA RELATORIO ##############")
print("############################################")

# Extract Data from Source
dados_hst = HST2TXT(hst2txt_exe_path, hst_data, 'dados')

pasta1 = dados_hst.load_hst_file_to_work(data_importacao, data_importacao)

if pasta1:
    print('Ok')
else:
    sys.exit()

dados_hst.extract_hst_file_to_txt()

# Export path location to work with data
work_path = dados_hst.work_path_location()
dados_path = dados_hst.new_data_path()


Dados_dados = Indusoft_data(work_path, dados_path)
Dados_dados.set_labels(["K1Q1_PA","K2Q1_PA","K3Q1_PA","K4Q1_PA","K5Q1_PA","K6Q1_PA","K7Q1_PA","K8Q1_PA","K9Q1_PA"])

df_dados_tabela = Dados_dados.get_database(data_importacao)
print(df_dados_tabela)


if not os.path.exists(os.path.join(main_path, 'avulso')):
    os.mkdir(os.path.join(main_path, 'avulso'))


export_path = os.path.join(main_path, 'avulso')
exporta_dados_diario(df_dados_tabela,data_importacao,export_path)
print("############################################")
print("############ RELATORIO PRONTO ##############")
print("############################################")

dados_hst.remove_work()
