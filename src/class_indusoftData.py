import os
from typing import Any

import pandas as pd
import numpy as np

class Indusoft_data():
    def __init__(self) -> None:
        pass

    def add_paths(self, in_work_path: str, in_data_path: str):
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
        '''Read data from extracted hst file'''
        
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
        header_list.insert(0,'Dia')
        # Make a dataframe with headers
        dados_brutos = pd.read_csv(file_path, sep='\t', names = header_list)
        return dados_brutos

    def calc_mean_by_hour(self, input_data):
        df_item = input_data

        lista_item = list()
        soma_item = 0   

        count = 0
        for k in range(len(df_item)):
            soma_item += df_item[k]
            count+=1
            if count > (int(len(df_item)/24)-1):
                lista_item.append(soma_item/(len(df_item)/24))
                count = 0
                soma_item = 0
        return lista_item
    
    def resume_by_hour(self, in_data_escolhida: str):
        
        label_dados = self.labels

        df_data = self.get_database(in_data_escolhida)

        lista_dados = list()
        for jj in range(len(label_dados)):
            lista_dados.append(self.calc_mean_by_hour(df_data[label_dados[jj]]))

        return lista_dados

    def get_dataframe_by_hour(self, in_data_escolhida: str, in_lista_labels: list, transpose_data = False):
        df = pd.DataFrame(self.resume_by_hour(in_data_escolhida))
        df_dados = df.transpose()
        df_dados.columns = in_lista_labels
        
        if transpose_data:
            df_dados = df_dados.transpose()

        return df_dados