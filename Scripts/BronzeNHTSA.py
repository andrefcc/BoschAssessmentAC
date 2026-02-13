from pathlib import Path
import requests

#url that points to the Safercar dataset provided by NHTSA
url_Safercar = "https://static.nhtsa.gov/nhtsa/downloads/Safercar/Safercar_data.csv"

#path where the file will be saved
Safercar_data_raw = Path("C:/Users/e709117/Downloads/Assessment/Bronze/Safercar_data.csv")
#if the folder doesn't already exist, it creates it
Safercar_data_raw.parent.mkdir(parents=True, exist_ok=True)

#Now we will request the file through an HTTP connection, and download it without loading the entire file into memory.
#Instead, it downloads it piece by piece (chunks of 1MB).
#This is safer for large datasets.
#There is also a timeout set, to prevent hanging if the server is not responding.
with requests.get(url_Safercar, stream=True, timeout=120) as r:
    r.raise_for_status()
    with open(Safercar_data_raw, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)

print(f"Saved to: {Safercar_data_raw}")