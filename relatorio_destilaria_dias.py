import sys
import warnings
import os
warnings.filterwarnings('ignore')
import pandas as pd

from src.class_hst2txt import HST2TXT
from src.class_indusoftData import Indusoft_data
from src.funcoes import exporta_dados_diario, gerar_lista_datas

from entradas import main_path, export_path, hst_destilaria, hst2txt_exe_path, data_importacao

# Copy of input data from entradas.
main_path = main_path
export_path = export_path
hst_destilaria = hst_destilaria

hst2txt_exe_path = hst2txt_exe_path
#data_importacao = data_importacao # dd-mm-yyyy

print("############################################")
print("############ RELATORIO DIARIO ##############")
print("############################################")

data_inicial = '01-01-2024'
data_final = '15-08-2024'

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
Dados_Destilaria.set_labels(["DE_AP1_FIT_VINHO_PV"])

lista_data = gerar_lista_datas(data_inicial, data_final)

lista_cabecalhos_export = ['Vazao Vinho (m3/h)',
                           'Data',
                           'Hora']

dados_list = pd.DataFrame(columns=lista_cabecalhos_export)


for data_importacao in lista_data:

    if Dados_Destilaria.verifica_arquivos(data_importacao):
        print(f'Arquivos do dia {data_importacao} existentes!')
    else:
        continue

    df_destilaria = Dados_Destilaria.get_dataframe_by_hour(data_importacao, Dados_Destilaria.labels)

    new_headers = ['Vazao Vinho (m3/h)']



    # Merge and change colunms names
    df_dados = df_destilaria
    df_dados.columns = new_headers
    df_dados['Vazao Vinho (m3/h)'] = df_dados['Vazao Vinho (m3/h)']*90/120
    
    df_dados['Data'] = pd.DataFrame([data_importacao ]* 24)
    df_dados['Hora'] = pd.DataFrame(list(range(24)))    
    
    #print(df_dados.head())
    #dados_list

    dados_list = dados_list.append(df_dados, ignore_index=True)

if not os.path.exists(os.path.join(main_path, 'avulso')):
    os.mkdir(os.path.join(main_path, 'avulso'))
    
export_path = os.path.join(main_path, 'avulso')

exporta_dados_diario(dados_list,data_importacao,export_path)

print("############################################")
print("############ RELATORIO PRONTO ##############")
print("############################################")

Destilaria.remove_work()