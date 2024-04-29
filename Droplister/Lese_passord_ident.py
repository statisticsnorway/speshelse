# +
import pandas as pd

from sqlalchemy import create_engine

import getpass
import os

import hjelpefunksjoner as hjfunk

from datetime import datetime
import re
# -

aar4 = 2023
aar2 = str(aar4)[-2:]
aarfør4 = aar4-1
aarfør2 = str(aarfør4)[-2:]

pass_stamme = (
        "/ssb/stamme01/fylkhels/speshelse/felles/brukere/"
        + str(aar4)
)
pass_sti = pass_stamme + r"/password_master.csv"

pw = pd.read_csv(pass_sti, encoding='latin1', sep=';')


# +
def find_latest_date_folder(path):
    date_pattern = re.compile(r'\d{2}\d{2}\d{2}')

    latest_date = None
    latest_folder = None

    for folder_name in os.listdir(path):
        folder_path = os.path.join(path, folder_name)
        
        if os.path.isdir(folder_path):
            match = date_pattern.search(folder_name)
            if match:
                folder_date = datetime.strptime(match.group(), '%d%m%y')
                if latest_date is None or folder_date > latest_date:
                    latest_date = folder_date
                    latest_folder = folder_name
    return latest_folder

stamme = "/ssb/stamme01/fylkhels/speshelse/felles/droplister/"
sti_bl = stamme + str(aar4) + "/"
sti_bl += find_latest_date_folder(sti_bl)


# +
entries = os.listdir(sti_bl)

filnavn = [entry for entry in entries if entry.startswith("Bruker")][0]

print(filnavn)
# -

br = pd.read_csv(sti_bl + "/" + filnavn, encoding='latin1', sep=';')

br = pd.merge(
    br,
    pw[['Username', 'Password']],
    how='outer',
    left_on='ORGNR_FORETAK',
    right_on='Username'
)

# ## Tabell med orgnr, skjema_type og identer

br = br.drop(columns=['HELSEREGION'])

m_39 = br['SKJEMA_TYPE'].str.contains("47")

priv = br[m_39].reset_index(drop=True).copy()
off = br[~m_39].reset_index(drop=True).copy()

til_oversikt_xlsx = (
    priv[["FORETAK_NAVN", "Username", "Password"]]
    .rename(
        columns={"FORETAK_NAVN": "foretak", "Username": "orgnr", "Password": "pinkode"}
    )
    .copy()
)

til_oversikt_csv = (
    priv[["ORGNR_FORETAK", "Username", "Password"]]
    .rename(
        columns={"ORGNR_FORETAK": "orgnr", "Username": "orgnr", "Password": "pinkode"}
    )
    .copy()
)

br

print(til_oversikt_csv.to_csv(sep=";", index=False))

print(til_oversikt_xlsx.to_csv(index=False, sep="\t"))


