"""Obtiene reporte de historia - Junta archivos CSV de historia y resume por agnio"""
import os
import pandas as pd
from sqlalchemy import create_engine
import cx_Oracle
from class_config import Config

cfg = Config()

cx_Oracle.init_oracle_client(lib_dir= cfg.get_par('lib_dir'))

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
        oracle_cnx_string = 'oracle+cx_oracle://{username}:{password}@{host_}:{port}/{database}'
        engine = create_engine(
            oracle_cnx_string.format(
                username = cfg.get_par('username'),
                password = cfg.get_par('password'),
                host_    = cfg.get_par('hostname'),
                port     = cfg.get_par('port'),
                database = cfg.get_par('database')
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
        data.to_csv(cfg.get_par('out_dir')+file__,index=False,
            sep=";",decimal=",",mode='a',header=True)
        self._log(f"Export ejecutado:{cfg.get_par('database')}{file__}")
        return data

    def export_metadata_counts(self):
        """function """
        sql_  = cfg.get_par('QUERY_METADATA_COUNTS')
        data  = self.execute_sql_source(sql_)
        data.to_csv(cfg.get_par('out_dir') + "METADATA_COUNTS.csv",index=False,sep=";",decimal=",")
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
