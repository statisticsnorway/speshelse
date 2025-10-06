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

til_lagring = True # Sett til True, hvis du vil lagre en ny fil

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

unike_nace_df = per.groupby("ORGNR_FRTK").agg(
    unike_nace07=("VIRK_NACE1_SN07", "unique"),
    unike_nace07_ant=("VIRK_NACE1_SN07", "nunique")
).reset_index()

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

df_final = ft.reduce(
    lambda left, right: pd.merge(
        left, right, on=["ORGNR_FRTK", "TJENESTE_KODE", "FORETAKSTYPE", 'HELSEREGION'], how="outer"
    ),
    dfs,
)

sql_str = hjfunk.lag_sql_str(df_final.ORGNR_FRTK.unique())

sporring_for = f"""
    SELECT ORGNR, NAVN, RECORD_ED, FORETAKS_NR_GDATO
    FROM DSBBASE.SSB_FORETAK
    WHERE
        ORGNR IN {sql_str} AND
        EXTRACT(YEAR FROM RECORD_ED) >= {aar4} AND
        EXTRACT(YEAR FROM FORETAKS_NR_GDATO) <= {aar4}
"""

foretak_navn = (
    hjfunk
    .les_sql(sporring_for, conn)
    .rename(columns={'ORGNR': 'ORGNR_FRTK'})
    .dropna(subset=["ORGNR_FRTK"])
    .drop_duplicates(subset=["ORGNR_FRTK"])
    .reset_index(drop=True)
    .drop(columns=["RECORD_ED", "FORETAKS_NR_GDATO"])
)

df_final = pd.merge(
    df_final,
    foretak_navn,
    how="left",
    on="ORGNR_FRTK",
)

df_final = df_final[[df_final.columns[0], df_final.columns[-1], *df_final.columns[1:-1]]]

# ## Tilpass fil

df_final_aarsverk = df_final[['ORGNR_FRTK', 'TJENESTE_KODE', 'AARSVERK', 'LONN']].copy()

df_final_aarsverk["LONN_tot"] = (
    df_final_aarsverk
    .groupby(["ORGNR_FRTK"])["LONN"]
    .transform("sum")
)

df_lonn_aos = df_final_aarsverk[df_final_aarsverk['TJENESTE_KODE'] == "AOS"][['ORGNR_FRTK', 'LONN']].copy()

df_final_aarsverk = pd.merge(
    df_final_aarsverk,
    df_lonn_aos,
    how='left',
    on='ORGNR_FRTK',
    suffixes=('', '_aos')
).fillna(0.0)

df_aarsverk_aos = df_final_aarsverk[df_final_aarsverk['TJENESTE_KODE'] == "AOS"][['ORGNR_FRTK', 'AARSVERK']].copy()

df_final_aarsverk = pd.merge(
    df_final_aarsverk,
    df_aarsverk_aos,
    how='left',
    on='ORGNR_FRTK',
    suffixes=('', '_aos')
).fillna(0.0)

df_final_aarsverk['LONN_tot_uten_aos'] = df_final_aarsverk['LONN_tot'] - df_final_aarsverk['LONN_aos']

df_final_aarsverk = df_final_aarsverk.drop(columns=['LONN_aos', 'LONN_tot'])

df_final_aarsverk['andel'] = df_final_aarsverk['LONN'] / df_final_aarsverk['LONN_tot_uten_aos']

df_final_aarsverk['AARSVERK_ny'] = df_final_aarsverk['AARSVERK'] + df_final_aarsverk['AARSVERK_aos'] * df_final_aarsverk['andel']

df_final_aarsverk = df_final_aarsverk[['ORGNR_FRTK', 'TJENESTE_KODE', 'AARSVERK_ny']]



df_final = pd.merge(
    df_final,
    df_final_aarsverk,
    how='left',
    on=['ORGNR_FRTK', 'TJENESTE_KODE']
)

# Tar ut AOS etter å ha fordel aarsverk utover de andre tjenestekodene.

df_final = df_final[df_final['TJENESTE_KODE'] != "AOS"].reset_index(drop=True)

# Lager noen interessante sammenstillinger av tall

df_final['SNITTLONN_millioner_ny'] = df_final['LONN'] / df_final['AARSVERK_ny'] / 1000

df_final['SNITTLONN_millioner'] = df_final['LONN'] / df_final['AARSVERK'] / 1000

verdi_kol = [
    "UTSKRIVNINGER",
    "OPPHOLDSDOGN",
    "OPPHOLDSDAGER",
    "POLIKLINIKK",
    "DOGNPLASSER",
    "SENGEDOGN",
    "TOT_UTG",
    "AARSVERK",
    "LONN"
]

df_final['sum_verdier_rad'] = round(df_final[verdi_kol].fillna(0.0).apply(abs).sum(axis=1), 1)


# +
def antall_0_eller_missing(row):
    return ((row != 0.0) & (~row.isnull())).sum()

# Legger til en ny kolonne som teller antall ikke-null og ikke-missing verdier
df_final['ant_num_verdier'] = df_final[verdi_kol].apply(antall_0_eller_missing, axis=1)
# -

# ## Koble på unike NACE

df_final = pd.merge(
    df_final,
    unike_nace_df,
    on="ORGNR_FRTK",
    how="left"
)

# # Lagring

excel_ark = {
    'Populasjonsanalyse': df_final
}

idag = pd.Timestamp.today().strftime('%Y-%m-%d-%H%M')

idag

if til_lagring:
    idag = pd.Timestamp.today().strftime('%Y-%m-%d-%H%M')
    sti = f"/ssb/stamme01/fylkhels/speshelse/felles/populasjon/{aar4}/populasjonsanalyse_{aar4}_{idag}.xlsx"
    lagre_excel(excel_ark, sti)











per[per["ORGNR_FRTK"] == "924212446"]

df_final[(df_final["OPPHOLDSDOGN"] > 0) & (df_final["AARSVERK"].isna())]  # <- Stemmer dette?? Dobbeltsjekk disse

df_final[df_final["ORGNR_FRTK"] == "953557088"]
