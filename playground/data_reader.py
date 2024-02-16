from typing import Callable
import os
import pyarrow.parquet as pq
from pickle_io import save_pkl, load_pkl
from abc import ABC, abstractmethod
from tqdm import tqdm
import numpy as np
#modules to read data from local folder
class Reader:
    def __init__(self, 
                 DATA_PATH: str,
                 in_Memory: bool=False):
        self.DATA_PATH = DATA_PATH
        self.in_Memory = in_Memory
        
    def ids(self,):
        ids = os.listdir(self.DATA_PATH)
        ids = [int(x.split('.')[0]) for x in ids]
        return ids
    
    def save_local(self, target_folder):
        print('hi')
        for id in tqdm(self.ids()):
            data = self.get(id)
            file_path = f"{target_folder}/{id}.pkl"
            if os.path.exists(file_path):
                print(f"{id}.pkl file already exist!")
            else:
                save_pkl(data, file_path)

    @abstractmethod
    def load_data(self,):
        pass
    
    @abstractmethod
    def get(self, id):
        pass

class PickleReader(Reader):
    #load data directly from local pikle file
    def __init__(self, 
                 DATA_PATH: str,
                 in_Memory: bool=False,
                 formators: list[Callable] = None):
        super().__init__(
            DATA_PATH = DATA_PATH,
            in_Memory = in_Memory
        )
        self.formators = formators
        if in_Memory:
            self.load_data()
            
    def __get(self, id):
        data = load_pkl(f'{self.DATA_PATH}/{id}.pkl')
        if self.formators is not None:
            for formator in self.formators:
                data = formator(data)
        return data

    def load_data(self,):
        self.data = {}
        for id in tqdm(self.ids()):
            self.data[id] = self.__get(id)

    def get(self, id):
        if self.in_Memory:
            return self.data[id]
        else:
            return self.__get(id)

class ParqReader(Reader):
    #load data directly from local pikle file
    def __init__(self, 
                 DATA_PATH: str,
                 in_Memory: bool=False,
                 formators: Callable = None):
        super().__init__(
            DATA_PATH = DATA_PATH,
            in_Memory = in_Memory
        )
        self.formators = formators
        if in_Memory:
            self.load_data()
            
    def __get(self, id):
        data = pq.read_table(f'{self.DATA_PATH}/{id}.parquet')
        if self.formators is not None:
            for formator in self.formators:
                data = formator(data)
        return data
    
    def load_data(self,):
        self.data = {}
        for id in tqdm(self.ids()):
            self.data[id] = self.__get(id)

    def get(self, id):
        if self.in_Memory:
            return self.data[id]
        else:
            return self.__get(id)