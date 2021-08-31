import pandas as pd
import requests
import io
import zipfile

class GDELTEvents:
    def __init__(self, input_path=None, output_path=None):
        self.input_path = input_path
        self.output_path = output_path
        self.gdelt_updates_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
    
    def check_new_release(self):
        resp = requests.get(self.gdelt_url)
        events_dataset_info = resp.text.split("\n")[0]
        events_dataset_url = events_dataset_info.split(" ")[-1]
        return events_dataset_url

    def download_csv(self, url, destination_directory):
        r = requests.get(url)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        z.extractall(destination_directory)
    
    def convert_to_parquet(self):
        csv = pd.read_csv("data/input/20210831110000.export.csv", header=None, sep="\t")
        print(csv.shape[0])
        rename_col = {}
        for i in csv.columns:
            rename_col[i] = f"col{i}"
        csv.rename(columns=rename_col, inplace=True)
        csv.to_parquet('output.parquet')
        print(rename_col)


if __name__ == "__main__":
    gdelt = GDELTEvents()
    # url = gdelt.check_new_release()
    # print(url)
    # gdelt.download_csv(url, "./data/input")
    gdelt.convert_to_parquet()