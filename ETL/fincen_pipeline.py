import pandas as pd
import os
import csv
import calendar
from datetime import datetime

from bamboo_lib.logger import logger
from bamboo_lib.helpers import grab_connector
from bamboo_lib.models import EasyPipeline, PipelineStep, Parameter
from bamboo_lib.steps import DownloadStep, UnzipStep, LoadStep


MONTH_MAP = {calendar.month_abbr[i]:i for i in range(1,13)}


class DateStep(PipelineStep):
    def run_step(self, prev, params):

        # prev es un generador de Python, así obtenemos todos los archivos que provengan del UnzipStep
        files = [f for f in prev]
        # El archivo que necesitamos se encuentra en el índice 2 de la lista
        df = pd.read_csv(files[2])

        date_col = df["begin_date"].append(df["end_date"])
        date_col = date_col.str.replace(", ", " ").fillna("")

        for d in date_col:
            if type(d) != str:
                print(d)

        print(type(date_col))
        print(date_col)
        

        #date_range = pd.date_range()
        return 0





class FincenPipeline(EasyPipeline):
    @staticmethod
    def parameter_list():
        return [
            Parameter("output-db", dtype=str),
            Parameter("ingest", dtype=bool)
        ]

    @staticmethod
    def steps(params):
        source_connector = grab_connector("../conns.yaml", "fincen-source")
        #db_connector = grab_connector("../conns.yaml", params.get("output-db"))

        dl_step = DownloadStep(connector=source_connector)
        unzip_step = UnzipStep(pattern=r"\.csv$")
        date_step = DateStep()

        
        #load_fact = LoadStep(
        #    table_name="tic_fact", 
        #    connector=db_connector, 
        #    if_exists="drop", 
        #    pk=["region_id"],
        #    dtype={"region_id":"UInt8","data_origin_id":"UInt8","response_id":"UInt8","year":"UInt8","percentage":"Float64"},
        #    nullable_list=[]
        #)

        if params.get("ingest")==True:
            steps = []
            #[open_step, tidy_step, region_step, load_region, variable_step, load_variable, fact_step, load_fact]
        else:
            steps = [dl_step, unzip_step, date_step]

        return steps


if __name__ == "__main__":
    fincen_pipeline = FincenPipeline()
    fincen_pipeline.run(
        {
            "output-db": "clickhouse-local",
            "ingest": False
        }
    )