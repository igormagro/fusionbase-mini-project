from typing import Tuple
from datetime import datetime
import pandas as pd
import requests
import io
import os
import zipfile
import schedule

GDELT_EVENTS_DATABASE_COLUMNS = [
    "GlobalEventID",
    "Day",
    "MonthYear",
    "Year",
    "FractionDate",
    "Actor1Code",
    "Actor1Name",
    "Actor1CountryCode",
    "Actor1KnownGroupCode",
    "Actor1EthnicCode",
    "Actor1Religion1Code",
    "Actor1Religion2Code",
    "Actor1Type1Code",
    "Actor1Type2Code",
    "Actor1Type3Code",
    "Actor2Code",
    "Actor2Name",
    "Actor2CountryCode",
    "Actor2KnownGroupCode",
    "Actor2EthnicCode",
    "Actor2Religion1Code",
    "Actor2Religion2Code",
    "Actor2Type1Code",
    "Actor2Type2Code",
    "Actor2Type3Code",
    "IsRootEvent",
    "EventCode",
    "EventBaseCode",
    "EventRootCode",
    "QuadClass",
    "GoldsteinScale",
    "NumMentions",
    "NumSources",
    "NumArticles",
    "AvgTone",
    "Actor1Geo_Type",
    "Actor1Geo_Fullname",
    "Actor1Geo_CountryCode",
    "Actor1Geo_ADM1Code",
    "Actor1Geo_ADM2Code",
    "Actor1Geo_Lat",
    "Actor1Geo_Long",
    "Actor1Geo_FeatureID",
    "Actor2Geo_Type",
    "Actor2Geo_Fullname",
    "Actor2Geo_CountryCode",
    "Actor2Geo_ADM1Code",
    "Actor2Geo_ADM2Code",
    "Actor2Geo_Lat",
    "Actor2Geo_Long",
    "Actor2Geo_FeatureID",
    "ActionGeo_Type",
    "ActionGeo_Fullname",
    "ActionGeo_CountryCode",
    "ActionGeo_ADM1Code",
    "ActionGeo_ADM2Code",
    "ActionGeo_Lat",
    "ActionGeo_Long",
    "ActionGeo_FeatureID",
    "DATEADDED",
    "SOURCEURL"
]
# as documented here: http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf


class GDELTEvents:
    def __init__(self, input_path=None, output_path=None, logs_path=None):
        self.input_path = input_path
        self.output_path = output_path
        self.gdelt_updates_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
        self.logs_path = logs_path

    def get_last_downloaded_file(self) -> None:
        with open(self.logs_path, "r") as file:
            for last_line in file:
                pass
        last_downloaded_file = last_line.split("\t")[0]
        self.last_downloaded_file = last_downloaded_file
        return

    def write_to_execution_log(self) -> None:
        exec_date = int(datetime.now().timestamp())
        updated_at_str = self.download_file_name.split(".")[0]
        updated_at = int(datetime.strptime(
            updated_at_str, "%Y%m%d%H%M%S").timestamp())
        with open(self.logs_path, "a") as f:
            f.write("\n")
            f.write(f"{self.download_file_name}\t{updated_at}\t{exec_date}")
        return

    def check_new_release(self) -> Tuple[bool, str]:
        self.get_last_downloaded_file()
        resp = requests.get(self.gdelt_updates_url)
        events_dataset_info = resp.text.split("\n")[0]
        events_dataset_url = events_dataset_info.split(" ")[-1]
        zip_file_name = events_dataset_url.split("/")[-1]
        available_file_name = zip_file_name.replace(".zip", "")

        return available_file_name == self.last_downloaded_file, events_dataset_url

    def download_csv(self, url) -> None:
        zip_file_name = url.split("/")[-1]
        self.download_file_name = zip_file_name.replace(".zip", "")
        res = requests.get(url)
        zip = zipfile.ZipFile(io.BytesIO(res.content))
        zip.extractall(self.input_path)
        self.write_to_execution_log()
        return

    def increment_and_convert_to_parquet(self) -> None:
        new_csv = pd.read_csv(
            f"{self.input_path}/{self.download_file_name}", header=None, sep="\t")
        new_csv.columns = GDELT_EVENTS_DATABASE_COLUMNS
        
        if not os.path.exists(f'{self.output_path}/output.parquet'):
            lines = 0
            new_parquet = new_csv
        else:
            parquet = pd.read_parquet(f'{self.output_path}/output.parquet')
            lines = parquet.shape[0]
            new_parquet = pd.concat([parquet, new_csv])
            new_parquet.drop_duplicates(inplace=True)

        new_parquet.to_parquet(f'{self.output_path}/output.parquet')
        print(new_parquet.shape[0] - lines, "lines inserted")
        return

    def run(self):
        print(datetime.now())
        new_release, zip_file_url = gdelt.check_new_release()
        if not new_release:
            self.download_csv(zip_file_url)
            print("Donwloaded:", self.download_file_name)
            self.increment_and_convert_to_parquet()
        else:
            print("Dataset not updated yet.")


if __name__ == "__main__":
    gdelt = GDELTEvents(
        "./data/landing",
        "./data/output",
        "./data/execution_logs.txt"
    )
    schedule.every(1).minute.do(gdelt.run)
    gdelt.run()
    while True:

        # Checks whether a scheduled task
        # is pending to run or not
        schedule.run_pending()


# s = "\n20210901003000.export.CSV\t20210901003000"
# open("./data/execution_logs.txt", "a").write(s)
