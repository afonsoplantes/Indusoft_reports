# Forma de execução
# C:/Users/afons/anaconda3/python.exe c:/Users/afons/Desktop/Github/Indusoft_reports/relatorio_diario.py
import os
from datetime import datetime, timedelta

'''
main_path = path to work with data
export_path = path to export the report
hst_gerador = path of hst folder of generator
hst_caldeira =  path of hst folder of caldeira
hst2txt_exe_path = path of hst2txt.exe
data_importacao = 15-05-2023

'''

main_path = 'C:\\Users\\afons\\Desktop\\Github\\Indusoft_reports'
export_path = 'S:\\99 - USINA\ELETRICA\\08-COGERACAO\\Relatorios de cogeracao\\'
#export_path = 'C:\\Users\\afons\\Safras armazem gerais\FILECLOUD - 99 - USINA\\ELETRICA\\08-COGERACAO\\Relatorios de cogeracao\\'
#export_path = os.path.join(main_path,'testes')

# Testes
#export_path = os.getcwd()

hst_gerador =  "Y:\\Hst"
hst_caldeira =  "Z:\\Hst"
hst_destilaria =  "X:\\HST"
hst_ete = "C:\\Users\\afons\\Desktop\\Github\\Indusoft_reports\\test_dataset\\ete"

hst_avulso = hst_gerador

#hst_gerador =  os.path.join(os.path.join(main_path,'test_dataset'),'gerador')
#hst_caldeira =  os.path.join(os.path.join(main_path,'test_dataset'),'caldeira')

print(hst_caldeira)

hst2txt_exe_path = "C:\\Users\\afons\\Desktop\\Github\\Indusoft_reports\\HST2TXT.exe"

data_hoje = datetime.today()
data_ontem = data_hoje - timedelta(1)
data_importacao = data_ontem.__format__('%d-%m-%Y')

