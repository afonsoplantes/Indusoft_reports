import sys
import warnings
warnings.filterwarnings('ignore')

from src.class_hst2txt import HST2TXT
from src.class_indusoftData import Indusoft_data
from src.funcoes import exporta_dados_diario, gerar_lista_datas

from entradas import main_path, export_path,hst_gerador, hst_caldeira, hst2txt_exe_path, data_importacao

# Copy of input data from entradas.
main_path = main_path
export_path = export_path
hst_gerador = hst_gerador
hst_caldeira = hst_caldeira
hst2txt_exe_path = hst2txt_exe_path
#data_importacao = data_importacao # dd-mm-yyyy

print("############################################")
print("############ RELATORIO DIARIO ##############")
print("############################################")

data_inicial = '01-01-2024'
data_final = '30-09-2024'

print("############################################")
print("############ INICIA RELATORIO ##############")
print("############################################")

# Extract Data from Source
Gerador = HST2TXT(hst2txt_exe_path, hst_gerador, 'Gerador_work')
Caldeira = HST2TXT(hst2txt_exe_path, hst_caldeira, 'Caldeira_work')

pasta1 = Gerador.load_hst_file_to_work(data_inicial, data_final)
pasta2 = Caldeira.load_hst_file_to_work(data_inicial, data_final)

if pasta1 & pasta2:
    print('Ok')
else:
    sys.exit()

Gerador.extract_hst_file_to_txt()
Caldeira.extract_hst_file_to_txt()

# Export path location to work with data
work_path = Gerador.work_path_location()
generator_path = Gerador.new_data_path()
caldeira_path = Caldeira.new_data_path()

Dados_Gerador = Indusoft_data(work_path, generator_path)
Dados_Gerador.set_labels(["K1Q1_PA","K2Q1_PA","K3Q1_PA","K4Q1_PA","K5Q1_PA",
                          "K6Q1_PA","K7Q1_PA","K8Q1_PA","K9Q1_PA"])

Dados_Caldeira = Indusoft_data(work_path, caldeira_path)
Dados_Caldeira.set_labels(["C2_VZ_VAPOR_CALD",
                        "C2_PRESSAO_VAPOR_COLETOR_SAIDA_GERAL_VAPOR"])

lista_data = gerar_lista_datas(data_inicial, data_final)

for data_importacao in lista_data:

    if Dados_Gerador.verifica_arquivos(data_importacao) & Dados_Caldeira.verifica_arquivos(data_importacao):
        print(f'Arquivos do dia {data_importacao} existentes!')
    else:
        continue

    df_gerador = Dados_Gerador.get_dataframe_by_hour(data_importacao, Dados_Gerador.labels)
    df_caldeira = Dados_Caldeira.get_dataframe_by_hour(data_importacao, Dados_Caldeira.labels)

    new_headers = ['Gerador Diesel (KWh)',
                'Gerador Vapor (KWh)',
                'Concessionaria (KWh)',
                'Caldeira (KWh)',
                'ETE Total(KWh)',
                'Fermentacao (KWh)',
                'Secador (KWh)',
                'Destilaria (KWh)',
                'Industria Total(KWh)',
                'Vazao Vapor Total (Ton)',
                'Pressao media (kgf/cm2)']

    # Merge and change colunms names
    df_dados = df_gerador.join(df_caldeira)
    df_dados.columns = new_headers

    #Correção da Base de tempo entre supervisório caldeira e gerador

    df_dados['Vazao Vapor Total (Ton)'] = df_dados['Vazao Vapor Total (Ton)']*(90/120)
    df_dados['Pressao media (kgf/cm2)'] = df_dados['Pressao media (kgf/cm2)']*(90/120)

    #Remove dados falsos de vazão de vapor, ou seja, quando pressão for menor que 1
    df_dados.loc[df_dados['Pressao media (kgf/cm2)'] < 1.5, 'Vazao Vapor Total (Ton)'] = 0.01
    
    # Fim da correçao da base de tempo
    df_dados['Consumo Total Planta (kwh)'] = df_dados['Industria Total(KWh)'] + df_dados['Destilaria (KWh)'] + df_dados['ETE Total(KWh)'] + df_dados['Fermentacao (KWh)'] + df_dados['Caldeira (KWh)'] + df_dados['Secador (KWh)']

    df_dados['Gerador Vapor (KWh)'] = df_dados['Consumo Total Planta (kwh)'] - df_dados['Concessionaria (KWh)']

    df_dados['Vazao Vapor Gerador (Ton)'] = df_dados['Gerador Vapor (KWh)']*(1/114)
    df_dados['Vazao Vapor Secador (Ton)'] = df_dados['Vazao Vapor Total (Ton)'] - df_dados['Gerador Vapor (KWh)']*(1/114)
    #df_dados['Vazao Vapor Escape (Ton)'] = df_dados['Vazao Vapor Gerador (Ton)']*(528.3/916.6)

    df_dados.loc[df_dados['Pressao media (kgf/cm2)'] < 1.5, 'Vazao Vapor Gerador (Ton)'] = 0.01
    df_dados.loc[df_dados['Pressao media (kgf/cm2)'] < 1.5, 'Vazao Vapor Secador (Ton)'] = 0

    #df_dados['ind-escritorio (kWh)'] = df_dados['Industria Total(KWh)']*0.01
    #df_dados['ind-laboratorio (kWh)'] = df_dados['Industria Total(KWh)']*0.02
    #df_dados['ind-cozimento (kWh)'] = df_dados['Industria Total(KWh)']*0.53
    #df_dados['ind-moinho (kWh)'] = df_dados['Industria Total(KWh)']*0.27
    #df_dados['ind-patriota (kWh)'] = df_dados['Industria Total(KWh)']*0.03
    #df_dados['ind-automacao (kWh)'] = df_dados['Industria Total(KWh)'] - df_dados[['ind-escritorio (kWh)','ind-laboratorio (kWh)','ind-cozimento (kWh)','ind-moinho (kWh)','ind-patriota (kWh)']].sum(axis=1)

    #df_dados['ete-adm (kWh)'] = df_dados['ETE Total(KWh)']*0.02
    #df_dados['ete-automacao (kWh)'] = df_dados['ETE Total(KWh)']*0.10
    #df_dados['ete-eletrica (kWh)'] = df_dados['ETE Total(KWh)']*0.02
    #df_dados['ete-mecanica (kWh)'] = df_dados['ETE Total(KWh)']*0.03
    #df_dados['ete-producao (kWh)'] = df_dados['ETE Total(KWh)'] - df_dados[['ete-adm (kWh)','ete-automacao (kWh)','ete-eletrica (kWh)','ete-mecanica (kWh)']].sum(axis=1)

    exporta_dados_diario(df_dados,data_importacao,export_path)

print("############################################")
print("############ RELATORIO PRONTO ##############")
print("############################################")

Caldeira.remove_work()
Gerador.remove_work()