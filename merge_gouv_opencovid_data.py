"""Module to merge the OpenCovid19-FR and data.gouv.fr datasets.

The function provided by this script (`download_france_data`) pulls the latest 
data from:

* https://raw.githubusercontent.com/opencovid19-fr/data/master/dist/chiffres-cles.csv
* https://www.data.gouv.fr/fr/datasets/r/f335f9ea-86e3-4ffa-9684-93c009d5e617

renames the columns of the data.gouv dataset to match the opencovid19 dataset and
merges the two datasets. The data is returned as a pandas dataframe.

If run from the command line as is this script will output a file merging the two
datasets:

    opencovid19-data-gouv-fr-chiffres-cles.csv



(Un)License:
------------

This is free and unencumbered software released into the public domain.

Anyone is free to copy, modify, publish, use, compile, sell, or
distribute this software, either in source code form or as a compiled
binary, for any purpose, commercial or non-commercial, and by any
means.

For more information, please refer to <http://unlicense.org/>
"""

import requests
import pandas as pd

DATA_GOUV_2_OPEN = {
    "date": "date",
    "granularite": "granularite",
    "maille_code": "maille_code",
    "maille_nom": "maille_nom",
    "rea": "reanimation",
    "hosp": "hospitalises",
    "dchosp": "deces",
    "incid_hosp": "nouvelles_hospitalisations",
    "incid_rea": "nouvelles_reanimations",
    "conf": "cas_confirmes",
    "esms_dc": "deces_ehpad",
    "esms_cas": "cas_confirmes_ehpad",
    "source_url": "source_url",
}


def download_france_data() -> pd.DataFrame:
    """Download and merges data from OpenCovid19-fr and data.gouv.fr
    """
    oc19_file = "opencovid19-fr-chiffres-cles.csv"
    gouv_file = "data-gouv-fr-chiffres-cles.csv"
    oc19_url = "https://raw.githubusercontent.com/opencovid19-fr/data/master/dist/chiffres-cles.csv"
    gouv_url = (
        "https://www.data.gouv.fr/fr/datasets/r/f335f9ea-86e3-4ffa-9684-93c009d5e617"
    )
    # run requests to download and save the data
    myfile = requests.get(oc19_url)
    with open(oc19_file, "wb") as f:
        f.write(myfile.content)
    file = requests.get(gouv_url)
    with open(gouv_file, "wb") as f:
        f.write(file.content)
    # Load both csv into pandas
    data = pd.read_csv(oc19_file)
    data_gouv = pd.read_csv(gouv_file)
    # Fill in some of the metadata that is not present in the government data
    data_gouv["granularite"] = "pays"
    data_gouv["maille_code"] = "FRA"
    data_gouv["maille_nom"] = "France"
    data["source_nom"] = "Santé publique France Data"
    data_gouv["source_url"] = gouv_url
    # merge and collate data
    data_gouv.rename(DATA_GOUV_2_OPEN, axis="columns", inplace=True)
    return pd.concat((data, data_gouv), join="outer")


if __name__ == "__main__":
    data = download_france_data()
    data.to_csv("opencovid19-data-gouv-fr-chiffres-cles.csv")
