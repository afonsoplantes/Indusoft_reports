import pandas as pd
import numpy as np

def exporta_dados_diario(df_dados, data_escolhida, main_path):
    
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