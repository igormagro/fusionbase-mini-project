from typing import Tuple
from datetime import datetime
import pandas as pd
import requests
import io
import os
import zipfile
import schedule


# as documented here: http://data.gdeltproject.org/documentation/GDELT-Event_Codebook-V2.0.pdf
with open("DATASET_COLUMNS.txt", "r") as f:
    gdelt_events_database_columns_str = f.read()

GDELT_EVENTS_DATABASE_COLUMNS = gdelt_events_database_columns_str.split("\n")


class GDELTEvents:
    def __init__(
        self,
        input_path: str = None,
        output_path: str = None,
        exec_logs_path: str = None,
        error_logs_path: str = None
    ):
        self.input_path = input_path
        self.output_path = output_path
        self.exec_logs_path = exec_logs_path
        self.error_logs_path = error_logs_path
        self.gdelt_updates_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

    def write_to_execution_log(self) -> None:
        exec_date = int(datetime.now().timestamp())
        updated_at_str = self.download_file_name.split(".")[0]
        updated_at = int(datetime.strptime(
            updated_at_str, "%Y%m%d%H%M%S").timestamp())

        row = [self.available_file_id,
               self.download_file_name, str(updated_at), str(exec_date)]

        with open(self.exec_logs_path, "a") as f:
            f.write("\n")
            f.write("\t".join(row))
        return

    def write_to_error_log(self, error: Exception) -> None:
        exec_date = int(datetime.now().timestamp())
        with open(self.error_logs_path, "a") as f:
            f.write(f"{exec_date}\tERROR: {error}")
            f.write("\n")
        return

    def get_last_downloaded_file(self) -> None:
        with open(self.exec_logs_path, "r") as file:
            for last_line in file:
                pass
       
        last_downloaded_file_id = last_line.split("\t")[0]

        self.last_downloaded_file_id = last_downloaded_file_id
        return

    def check_new_release(self) -> Tuple[bool, str]:
        self.get_last_downloaded_file()
        try:
            res = requests.get(self.gdelt_updates_url)
            res.raise_for_status()
        except Exception as e:
            self.write_to_error_log(e)
            raise Exception(e)
        else:
            events_dataset_info = res.text.split("\n")[0]
            self.available_file_id = events_dataset_info.split(" ")[1]
            events_dataset_url = events_dataset_info.split(" ")[-1]

        return (self.available_file_id != self.last_downloaded_file_id, events_dataset_url)

    def download_csv(self, url) -> None:
        zip_file_name = url.split("/")[-1]
        self.download_file_name = zip_file_name.replace(".zip", "")
        try:
            res = requests.get(url)
            res.raise_for_status()
        except Exception as e:
            self.write_to_error_log(e)
            raise Exception(e)
        else:
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
        try:
            new_release, zip_file_url = gdelt.check_new_release()
            if new_release:
                self.download_csv(zip_file_url)
                print("Downloaded:", self.download_file_name)
                self.increment_and_convert_to_parquet()
            else:
                print("Dataset not updated yet.")
        except Exception as e:
            print("ERROR:", e)


if __name__ == "__main__":
    gdelt = GDELTEvents(
        "./data/landing",
        "./data/output",
        "./logs/execution_log.txt",
        "./logs/error_log.txt"
    )
    schedule.every(15).minute.do(gdelt.run)
    gdelt.run()

    while True:
        schedule.run_pending()
