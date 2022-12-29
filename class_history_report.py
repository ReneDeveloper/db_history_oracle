"""Obtiene reporte de historia - Junta archivos CSV de historia y resume por agnio"""
import os
import pandas as pd
from class_config import Config
from sqlalchemy import create_engine
import cx_Oracle
cfg = Config()

cx_Oracle.init_oracle_client(lib_dir= cfg.getPar('lib_dir'))

class HistoryReport():
    """Procesa la historia de servidor"""
    def __init__(self,name__):
        self.name_ = name__
        """Inicializa, con el inicio del nombre de los archivos, ej: caso HIST"""

    def _log(self,var):
        """function _log""" 
        print(f"HistoryReport:{var}")

    def get_engine_source(self):
        """function """
        oracle_connection_string = 'oracle+cx_oracle://{username}:{password}@{host_}:{port}/{database}'
        engine = create_engine(
            oracle_connection_string.format(
                username = cfg.getPar('username'),
                password = cfg.getPar('password'),
                host_    = cfg.getPar('hostname'),
                port     = cfg.getPar('port'),
                database = cfg.getPar('database')
            )
            ,pool_timeout=99999
        )
        return engine

    def execute_sql_source(self,sql__):
        """function """
        engine = self.get_engine_source()
        data = pd.read_sql(sql__, engine)
        return data

    def export_sql_csv_header(self, sql__, file__):
        """function """
        data = self.execute_sql_source(sql__)
        data.to_csv(cfg.getPar('out_dir')+file__,index=False,sep=";",decimal=",",mode='a',header=True)
        self._log(f"Export ejecutado:{cfg.getPar('database')}{file__}")
        return data

    def procesa_history_files(self):
        """procesa_history_files: """ 
        print(f"Procesando archivos de historia:BBB_HISTORIA_{self.name_}")

        ruta = "C:/Users/rcastillosi/__SQL_DATABASE_STATS__/__EXPORT_DATA__/"

        # Crear una lista vacía para almacenar los dataframes de cada archivo
        df_list = []

        files = os.listdir(ruta)

        # Iterar sobre cada archivo y imprimir el nombre del archivo si comienza con "HIST"
        for file in files:
            if file.startswith("HIST"):
                print(file)
                df_list.append(pd.read_csv(f'{ruta}{file}', header=None))

        # Concatenar todos los dataframes en un único dataframe
        df_final = pd.concat(df_list)

        # Escribir el dataframe final a un nuevo archivo CSV
        print("antes de escribir")
        out_csv = df_final.to_csv(f"{ruta}BBB_HISTORIA_{self.name_}.csv", index=False)
        print(f"despues de escribir:out_csv:{out_csv}")
