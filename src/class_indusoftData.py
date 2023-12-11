import os
from typing import Any

import pandas as pd
import numpy as np

class Indusoft_data():
    def __init__(self, in_work_path: str, in_data_path: str) -> None:
        self.work_path = in_work_path
        self.data_path = in_data_path        
    
    def set_labels(self, in_label_dados_escolhidos: list):
        self.labels = in_label_dados_escolhidos

    def set_hst_filename(self, in_data_escolhida: str):
        '''Data format dd-mm-yyyy'''
        ano = in_data_escolhida[-2:]
        mes = in_data_escolhida[3:5]
        dia = in_data_escolhida[0:2]
        return  "01" + ano + mes + dia
    
    def flatten(self, t):
        '''Reduce a mxn matrix into a mn matrix'''
        return [item for sublist in t for item in sublist]
    
    def verifica_arquivos(self, in_data_escolhida:str):
        file_name = self.set_hst_filename(in_data_escolhida)
        txt_file_path= os.path.join(self.data_path, os.path.join('TXT_FILES', f'{file_name}.txt'))
        header_file_path = os.path.join(self.data_path, os.path.join('HDR_FILES', f'{file_name}.hdr'))
        
        if os.path.exists(txt_file_path) & os.path.exists(header_file_path):
            return True
        else:
            return False

    def get_database(self, in_data_escolhida: str):
        '''
        Read data from extracted hst file

        Inform date as -> dd-mm-yyyy
        '''
        
        file_name = self.set_hst_filename(in_data_escolhida)
        # Join File Path
        file_path = os.path.join(self.data_path, os.path.join('TXT_FILES', f'{file_name}.txt'))
        header_path = os.path.join(self.data_path, os.path.join('HDR_FILES', f'{file_name}.hdr'))

        # Read header File
        header_list = []
        ## Purge dirty data from header file
        cabecas = pd.read_csv(header_path, names = ['cabecas'])
        cabecas['cabecas'] = cabecas['cabecas'].str.strip()
        cabecas['cabecas'] = cabecas['cabecas'].str.replace('.','_')
        lista = cabecas[1:].values.tolist()
        header_list = self.flatten(lista)
        header_list.insert(0,'Hora')
        header_list.insert(0,'Data')
        dados_brutos = pd.read_csv(file_path, sep='\t', names = header_list)

        dados_brutos['Data'] = pd.to_datetime(dados_brutos['Data'], format='%d/%m/%Y')
        dados_brutos['Data_Hora'] = dados_brutos['Data'] + pd.to_timedelta(dados_brutos['Hora'])
        primeiro_item = dados_brutos['Data_Hora'].iloc[0]
        if 23 <= primeiro_item.hour < 24:
            dados_brutos['Data_Hora'] = dados_brutos['Data_Hora'] + pd.DateOffset(hours=1)
        # Remove as colunas 'Data' e 'Hora'.
        dados_brutos = dados_brutos.drop(['Data', 'Hora'], axis=1)
        # Obtém a ordem atual das colunas.
        colunas = dados_brutos.columns.tolist()
        # Move a coluna 'Data_Hora' para a primeira posição.
        colunas = ['Data_Hora'] + [coluna for coluna in colunas if coluna != 'Data_Hora']
        dados_brutos = dados_brutos[colunas]
        return dados_brutos
    
    def get_dataframe_by_hour(self, in_data_escolhida: str, in_lista_labels: list, transpose_data = False):
        """
            in_data_escolhida -> str of the chosen date as dd-mm-yyyy

            in_lista_labels -> list of the labels you choose to show

            transpose_data -> default false, choose if you want to transpose.
        """
        
        df = self.get_database(in_data_escolhida) 
        points_per_hour = 90
        df_mean = df.groupby(df['Data_Hora'].dt.hour).sum().reset_index()
        df_dados = df_mean[in_lista_labels]/points_per_hour 
        if transpose_data:
            df_dados = df_dados.transpose()
        return df_dados
    
    def get_database_max(self, in_data_escolhida: str, transpose_data = False):


        df = self.get_database(in_data_escolhida)

        df = df[self.labels].max()

        if transpose_data:
            df = df.transpose()

        return df