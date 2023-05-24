import os
import subprocess
from tqdm import tqdm
import shutil
from datetime import datetime, timedelta

class HST2TXT():
    def __init__(self, in_hst2txt_exe_path: str, in_hst_data_path: str, in_nome_pasta_copia: str) -> None:
        self.hst2txt_exe_path = in_hst2txt_exe_path
        self.hst_folder_path = in_hst_data_path
        self.nome_pasta_hst = in_nome_pasta_copia
    def work_path_location(self):
        return os.getcwd()
    
    def new_data_path(self):
        return os.path.join(os.getcwd(),self.nome_pasta_hst)
    
    def cria_pasta_hst(self, hst_files: list):
        try:
            os.mkdir(self.new_data_path())
        except:
            shutil.rmtree(self.new_data_path(), ignore_errors=False)
            os.mkdir(self.new_data_path())
        finally:
            for hst in hst_files:
                shutil.copy(hst,self.new_data_path())
    def remove_work(self):
        try:
            shutil.rmtree(self.new_data_path(), ignore_errors=False)
        except:
            print(f'A pasta {self.new_data_path()} não existe.')

    def filtra_arquivos_hst(self, arquivos, in_data_inicio:str, in_data_fim:str):       
        data_inicial = datetime.strptime(in_data_inicio, '%d-%m-%Y')
        data_final = datetime.strptime(in_data_fim, '%d-%m-%Y')
        arquivos_filtrados = []
        while data_inicial <= data_final:
            ano = data_inicial.__format__('%d-%m-%Y')[-2:]
            mes = data_inicial.__format__('%d-%m-%Y')[3:5]
            dia = data_inicial.__format__('%d-%m-%Y')[0:2]
            arquivo_desejado = os.path.join(self.hst_folder_path, f'01{ano}{mes}{dia}.hst')
            if arquivo_desejado in arquivos:
                arquivos_filtrados.append(arquivo_desejado)
            data_inicial += timedelta(days=1)
        return arquivos_filtrados
        
    def load_hst_file_to_work(self, in_data_inicio = None, in_data_fim = None):
        hst_files = [os.path.join(self.hst_folder_path,f) for f in os.listdir(self.hst_folder_path) if f.endswith(".hst")]
        hst_files_filtros = self.filtra_arquivos_hst(hst_files,in_data_inicio, in_data_fim)
        if len(hst_files) == 0:
            print('Pasta Vasia')
            return False
        else:
            self.cria_pasta_hst(hst_files_filtros)
            print('Arquivos Carregados')
            return True
    
    def make_extraction_path(self, files: list, extention: str):
        folder_location = self.new_data_path()
        try:
            os.mkdir(os.path.join(folder_location,extention))
        except:
            shutil.rmtree(os.path.join(folder_location,extention), ignore_errors=True)
            os.mkdir(os.path.join(folder_location,extention))
        finally:
            for hdr in files:
                shutil.move(hdr,os.path.join(folder_location,extention))
    
    def remove_file(self, files):
        if len(files)>0:
            [os.remove(file) for file in files]
        else:
            print('Pasta Vazia')

    def extract_hst_file_to_txt(self):
        folder_location = self.new_data_path()

        hst_files = [os.path.join(folder_location,f) for f in os.listdir(folder_location) if f.endswith(".hst")]
        
        bar = tqdm(hst_files,unit="file")   
        for file in bar:
            bar.set_description("File is processing : {}".format(file))
            subprocess.call([self.hst2txt_exe_path,file],stdout=subprocess.DEVNULL,stderr=subprocess.STDOUT)

        hdr_files = [os.path.join(folder_location,f) for f in os.listdir(folder_location) if f.endswith(".hdr")]
        txt_files = [f.replace(".hdr",".txt") for f in hdr_files]

        self.make_extraction_path(hdr_files, "HDR_FILES")
        self.make_extraction_path(txt_files, "TXT_FILES")
        self.remove_file(hst_files)

        print('##################################################')
        print('############ Extração concluida ##################')
        print('##################################################')