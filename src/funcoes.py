import os
import pandas as pd
import numpy as np
import csv

from datetime import datetime, timedelta

def exporta_dados_diario(df_dados, data_escolhida, main_path):
    
   ano = data_escolhida[-2:]
   mes = data_escolhida[3:5]
   dia = data_escolhida[0:2]
    
   export_path = os.path.join(main_path,f'safras20{ano}')
   try:
      os.mkdir(export_path)
   except:
      print(f"Caminho de Exportação: {export_path}")

   export_path = os.path.join(export_path, 'relatorio_diario')
   try:
      os.mkdir(export_path)
   except:
      print(f"Caminho de Exportação: {export_path}")

   export_path = os.path.join(export_path, mes)
   try:
      os.mkdir(export_path)
   except:
      print(f'Data de Exportação: {data_escolhida}')
   
   file_name = f'20{ano}-{mes}-{dia}.xlsx'

   with pd.ExcelWriter(os.path.join(export_path, file_name)) as writer:
      df_dados.to_excel(writer, sheet_name="dados_cogeracao")


def gerar_lista_datas(data_inicial_str: str, data_final_str: str):
    data_inicial = datetime.strptime(data_inicial_str, '%d-%m-%Y')
    data_final = datetime.strptime(data_final_str, '%d-%m-%Y')

    lista_datas = []

    while data_inicial <= data_final:
        lista_datas.append(data_inicial.strftime('%d-%m-%Y'))
        data_inicial += timedelta(days=1)

    return lista_datas

### RELATORIO DE DEMANADA relatorio_demanda
def export_lists_csv(data_importacao, serie_valores, export_path):
   
   ano = data_importacao[-2:]
   mes = data_importacao[3:5]
   dia = data_importacao[0:2]

   fields = ['DATA (kva)','Diesel (kva)','Turbogerador (kva)','Concessionaria (kva)','Caldeira (kva)','ETE (kva)','Fermentacao (kva)','Secador (kva)','Destilaria (kva)','Industria (kva)']
   rows = ['{}-{}-20{}'.format(dia,mes,ano)]

   for valor in serie_valores:
      rows.append(str(valor).replace('.',','))

   dados = []

   dados.append(fields)
   dados.append(rows)

   print(dados)

   export_path = os.path.join(export_path,f'safras20{ano}')
   try:
      os.mkdir(export_path)
   except:
      print(f"Caminho de Exportação: {export_path}")

   export_path = os.path.join(export_path, 'relatorio_demanda')
   try:
      os.mkdir(export_path)
   except:
      print(f"Caminho de Exportação: {export_path}")

   export_path = os.path.join(export_path, mes)
   try:
      os.mkdir(export_path)
   except:
      print(f'Data de Exportação: {data_importacao}')
   
   file_name = f'20{ano}-{mes}-{dia}.csv'

   caminho_csv = os.path.join(export_path, file_name)

   # Escrever os dados para o arquivo CSV
   with open(caminho_csv, 'w', newline='', encoding='utf-8') as arquivo_csv:
      escritor_csv = csv.writer(arquivo_csv, delimiter='\t')
      escritor_csv.writerows(dados)
         
    
   
   
