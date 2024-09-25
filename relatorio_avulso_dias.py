import sys
import warnings
import os
warnings.filterwarnings('ignore')
import pandas as pd

from src.class_hst2txt import HST2TXT
from src.class_indusoftData import Indusoft_data
from src.funcoes import exporta_dados_diario, gerar_lista_datas

from entradas import main_path, export_path, hst_avulso, hst2txt_exe_path, data_importacao

# Copy of input data from entradas.
main_path = main_path
export_path = export_path
hst_destilaria = hst_avulso

hst2txt_exe_path = hst2txt_exe_path
#data_importacao = data_importacao # dd-mm-yyyy

print("############################################")
print("############ RELATORIO DIARIO ##############")
print("############################################")

data_inicial = '14-09-2024'
data_final = '24-09-2024'

print("############################################")
print("############ INICIA RELATORIO ##############")
print("############################################")

# Extract Data from Source
Destilaria = HST2TXT(hst2txt_exe_path, hst_destilaria, 'Destilaria_work')

pasta1 = Destilaria.load_hst_file_to_work(data_inicial, data_final)

if pasta1:
    print('Ok')
else:
    sys.exit()

Destilaria.extract_hst_file_to_txt()

# Export path location to work with data
work_path = Destilaria.work_path_location()
destilaria_path = Destilaria.new_data_path()

Dados_Destilaria = Indusoft_data(work_path, destilaria_path)
Dados_Destilaria.set_labels(["ETE_LIND_CPH01_READ","ETE_LIND_CPH02_PID_READ"])

lista_data = gerar_lista_datas(data_inicial, data_final)


for data_importacao in lista_data:

    if Dados_Destilaria.verifica_arquivos(data_importacao):
        print(f'Arquivos do dia {data_importacao} existentes!')
    else:
        continue

    df_destilaria = Dados_Destilaria.get_database(data_importacao)

    # Merge and change colunms names
    df_dados = df_destilaria
    
    if not os.path.exists(os.path.join(main_path, 'avulso')):
        os.mkdir(os.path.join(main_path, 'avulso'))
    
    export_path = os.path.join(main_path, 'avulso')
    exporta_dados_diario(df_dados,data_importacao,export_path)

print("############################################")
print("############ RELATORIO PRONTO ##############")
print("############################################")

Destilaria.remove_work()