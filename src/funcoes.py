import os
import pandas as pd
import numpy as np

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

   try:
      os.path.join(export_path, mes)
   except:
      print(f'Data de Exportação: {data_escolhida}')
   
   file_name = f'20{ano}-{mes}-{dia}.xlsx'

   with pd.ExcelWriter(os.path.join(export_path, file_name)) as writer:
      df_dados.to_excel(writer, sheet_name="dados_cogeracao")