# %xmode minimal

import sys
sys.path.insert(0, '..')

import Droplister.hjelpefunksjoner as hjfunk
from functions.hjelpefunksjoner import lagre_excel

import getpass
import datetime as dt
import requests

import pandas as pd
import numpy as np
import functools as ft
import getpass
from sqlalchemy import create_engine
import datetime as dt

""
til_lagring = True # Sett til True, hvis du vil lagre en ny fil
""

username = getpass.getuser()
dsn = "DB1P"
try:
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")
except:
    password = getpass.getpass(prompt='Oracle-passord: ')
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")

conn = engine.connect()

pd.options.display.float_format = '{:.1f}'.format

aar4 = '2024'
aar2 = aar4[-2:]

# Ta høyde for siste versjon av filene
sti_akt   = f"/ssb/stamme01/fylkhels/speshelse/aktivitet_dapla/klargjorte-data/{aar4}/aktivitet_klargjort_p{aar4}.parquet"
sti_rgn0x = f"/ssb/stamme01/fylkhels/speshelse/regnskap_dapla/statistikk/{aar4}/resultat-statistikk-helseforetak_p{aar4}.parquet"
sti_per   = f"/ssb/stamme01/fylkhels/speshelse/personell_dapla/klargjorte-data/{aar4}/personell-klargjort_p{aar4}.parquet"

# ## Regnskap

rgn0x = pd.read_parquet(sti_rgn0x)



# +
# LAB, PTR, PBF, ADM finnes ikke de andre filene. -> settes til SOM

tjenester_til_SOM = [
    "LAB",
    "PTR",
    "PBF",
    "ADM",
    "REH"
]
m_til_SOM = rgn0x['TJENESTE_KODE'].isin(tjenester_til_SOM)
rgn0x.loc[m_til_SOM, 'TJENESTE_KODE'] = "SOM"
# -

rgn0x = rgn0x.loc[(rgn0x["TJENESTE_KODE"] == "TOT") & (rgn0x["variabel"].isin(["K05", "K00"]))]

rgn0x = rgn0x.drop(columns=["NAVN_STATBANK", "faste_priser_2b", "AARGANG"])

rgn0x = (
    rgn0x.pivot(
        index=["ORGNR_STATBANK", "HELSEREGION", "TJENESTE_KODE", "FORETAKSTYPE"],
        columns="variabel",
        values="verdi",
    )
    .reset_index()
    .rename(columns={"K00": "TOT_UTG",
                     "K05": "LONN",
                     "ORGNR_STATBANK": "ORGNR_FRTK"})
)

# # Aktivitet

akt = pd.read_parquet(sti_akt)

akt = akt.drop(columns=['NAVN_FRTK', 'TJENESTE_NAVN', 'RHF'])

akt.loc[akt["TJENESTE_KODE"] == "REH", "TJENESTE_KODE"] = "SOM"

as_mask = akt['FORETAKSTYPE'] == "Avtalespesialister"
akt.loc[as_mask, 'ORGNR_FRTK'] = akt.loc[as_mask, 'ORGNR_STATBANK'] 

if len(akt) > 0:
    akt = akt.groupby(['ORGNR_FRTK', 'TJENESTE_KODE', 'FORETAKSTYPE', 'HELSEREGION', 'ORGNR_STATBANK']).sum(numeric_only=True).reset_index()

# # Personell

per = pd.read_parquet(sti_per)

per.loc[per["TJENESTE_KODE"] == "REH", "TJENESTE_KODE"] = "SOM"

per = (
    per[
        [
            "ORGNR_FRTK",
            "TJENESTE_KODE",
            "AARSVERK",
            "FORETAKSTYPE",
            "HELSEREGION",
            "ORGNR_STATBANK",
        ]
    ]
    .groupby(
        ["ORGNR_FRTK", "TJENESTE_KODE", "FORETAKSTYPE", "HELSEREGION", "ORGNR_STATBANK"]
    )
    .sum()
    .reset_index()
)

per_tot = (per.groupby(
                ["ORGNR_FRTK", "FORETAKSTYPE", "HELSEREGION", "ORGNR_STATBANK"]
            )["AARSVERK"]
            .sum()).reset_index()

per_tot["TJENESTE_KODE"] = "TOT"

per = pd.concat([per, per_tot], ignore_index=True)

# # Merge

akt = akt.drop(columns="ORGNR_STATBANK")
per = per.drop(columns="ORGNR_STATBANK")

dfs = [akt, rgn0x, per]

del df_final

df_final = ft.reduce(
    lambda left, right: pd.merge(
        left, right, on=["ORGNR_FRTK", "TJENESTE_KODE", "FORETAKSTYPE", 'HELSEREGION'], how="outer"
    ),
    dfs,
)

df_final

sql_str = hjfunk.lag_sql_str(df_final.ORGNR_FRTK.unique())

sporring_for = f"""
    SELECT ORGNR, NAVN, RECORD_ED, FORETAKS_NR_GDATO
    FROM DSBBASE.SSB_FORETAK
    WHERE
        ORGNR IN {sql_str} AND
        EXTRACT(YEAR FROM RECORD_ED) >= {aar4} AND
        EXTRACT(YEAR FROM FORETAKS_NR_GDATO) <= {aar4}
"""

sporring_for = f"""
    SELECT FORETAKS_NR, ORGNR, NAVN
    FROM DSBBASE.SSB_FORETAK
"""

foretak_navn = hjfunk.les_sql(sporring_for, conn).rename(columns={'ORGNR': 'ORGNR_FRTK'})

df_final
