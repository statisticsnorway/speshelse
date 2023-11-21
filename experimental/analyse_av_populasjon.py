# -*- coding: utf-8 -*-
# # Prep

# ## Pakker og tilganger

import sys
sys.path.insert(0, '..')

import Droplister.hjelpefunksjoner as hjfunk
from functions.hjelpefunksjoner import lagre_excel

# +
import pandas as pd

import getpass
import datetime as dt
import requests

from sqlalchemy import create_engine
import getpass
# -

################################################################
til_lagring = True # Sett til True, hvis du vil lagre en ny fil
################################################################

import functools as ft

pd.options.display.float_format = '{:.1f}'.format

username = getpass.getuser()
password = getpass.getpass(prompt='Oracle-passord: ')
dsn = "DB1P"

# +
engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")

# Opprett en tilkobling fra motoren
conn = engine.connect()
# -


aar4 = '2022'
aar2 = aar4[-2:]


def rapport_df(df):
    display(df.info())
    display(df.sample(2))
    df.filter(like="ORG").nunique()


# # Innlasting av filer

sti_akt   = f"/ssb/stamme01/fylkhels/speshelse/aktivitet/{aar4}/masterfil/aktivitet_masterfil_{aar4}.parquet"
sti_rgn0x = f"/ssb/stamme01/fylkhels/speshelse/regnskap/{aar4}/masterfil/helse0x_masterfil_{aar4}.parquet"
sti_rgn39 = f"/ssb/stamme01/fylkhels/speshelse/regnskap/{aar4}/masterfil/helse39_masterfil_{aar4}.parquet"
sti_per   = f"/ssb/stamme01/fylkhels/speshelse/personell/{aar4}/personell/masterfil/personell_masterfil_{aar4}.parquet"

akt = pd.read_parquet(sti_akt)
rgn0x = pd.read_parquet(sti_rgn0x)
rgn39 = pd.read_parquet(sti_rgn39)
per = pd.read_parquet(sti_per)

# # Bearbeiding av filer

HF, RHF, phob, rfss, rfss2, rfss3, rapporteringsenheter = hjfunk.hent_enheter_fra_klass(
    aar4
)

rfs = pd.concat([rfss, rfss2, rfss3], ignore_index=True)

rfs['FORETAKSTYPE'] = 'Støtteforetak'
HF['FORETAKSTYPE'] = 'HF'
phob['FORETAKSTYPE'] = 'Oppdrag'
RHF['FORETAKSTYPE'] = 'RHF'

foretakstyper = (
    pd.concat([RHF, HF, phob, rfs], ignore_index=True)
    .drop(columns=["HELSEREGION", "NAVN_KLASS"])
    .rename(columns={"ORGNR_FORETAK": "ORGNR_FRTK"})
)

# ## Aktivitet

akt.FORETAKSTYPE.value_counts()

akt.columns

akt = (
    akt
    .drop(columns=['NAVN_FRTK', 'TJENESTE_NAVN', 'RHF'])
)

as_mask = akt['FORETAKSTYPE'] == "Avtalespesialister"
akt.loc[as_mask, 'ORGNR_FRTK'] = akt.loc[as_mask, 'ORGNR_STATBANK'] 

akt = akt.groupby(['ORGNR_FRTK', 'TJENESTE_KODE', 'FORETAKSTYPE', 'HELSEREGION', 'ORGNR_STATBANK']).sum(numeric_only=True).reset_index()

rapport_df(akt)

# ## Regnskap skjema0X

# Merknader:
# - ADM i regnskapsfilene er kun data for RHFene
#     - I personellfilen er alle årsverk som ikke er pasientrettet blir regnet som ADM
#

# +
# LAB, PTR, PBF, ADM finnes ikke de andre filene. -> settes til SOM

tjenester_til_SOM = [
    "LAB",
    "PTR",
    "PBF",
    "ADM",
]
m_til_SOM = rgn0x['TJENESTE_KODE'].isin(tjenester_til_SOM)
# rgn0x = rgn0x[~rgn0x['tjenester_til_SOM'].isin(tjenester_tas_ut)].copy()
rgn0x.loc[m_til_SOM, 'TJENESTE_KODE'] = "SOM"
# -

rgn0x = rgn0x[["ORGNR_FRTK", "TJENESTE_KODE", "TOT_UTG", 'HELSEREGION', "ORGNR_STATBANK", "LONN"]]

rgn0x = rgn0x.groupby(['ORGNR_FRTK', 'TJENESTE_KODE', 'HELSEREGION', "ORGNR_STATBANK"]).sum().reset_index()

rgn0x = pd.merge(
    rgn0x,
    foretakstyper,
    on='ORGNR_FRTK',
    how='left'
)

rapport_df(rgn0x)

# ---

# ## Regnskap skjema39

# Hvis `SN07_1 == 86.107`, skal `funksjon`/`TJENESTE` være `REH`
m_86107 = rgn39['SN07_1'] == '86.107'
rgn39.loc[m_86107, 'TJENESTE_KODE'] = "REH"

rgn39 = rgn39[["ORGNR_VIRK", "TOT_UTG", "TJENESTE_KODE", "ORGNR_FRTK", 'FORETAKSTYPE', "HELSEREGION", "ORGNR_STATBANK", "LONN"]]

rgn39 = (
    rgn39.groupby(["ORGNR_FRTK", "TJENESTE_KODE", "FORETAKSTYPE", "HELSEREGION", "ORGNR_STATBANK"])[['TOT_UTG', 'LONN']].sum()
    .reset_index()
)

rapport_df(rgn39)

# ## Personell

per = (
    per[['ORGNR_FRTK', 'TJENESTE_KODE', 'AARSVERK', 'FORETAKSTYPE', 'HELSEREGION', "ORGNR_STATBANK"]]
    .groupby(['ORGNR_FRTK', 'TJENESTE_KODE', 'FORETAKSTYPE', 'HELSEREGION', "ORGNR_STATBANK"]).sum()
    .reset_index()
)

rapport_df(per)

# # Merge alle fire mastertabeller på foretak

dfs = [akt, rgn0x, rgn39, per]

for df in dfs:
    print(df.columns.to_list())

df_final = ft.reduce(
    lambda left, right: pd.merge(
        left, right, on=["ORGNR_FRTK", "TJENESTE_KODE", "FORETAKSTYPE", 'HELSEREGION', "ORGNR_STATBANK"], how="outer"
    ),
    dfs,
)

df_final['TOT_UTG'] = df_final['TOT_UTG_x'].astype(float).fillna(0.0) + df_final['TOT_UTG_y'].astype(float).fillna(0.0)
df_final['LONN'] = df_final['LONN_x'].astype(float).fillna(0.0) + df_final['LONN_y'].astype(float).fillna(0.0)
df_final = df_final.drop(columns=['LONN_x', 'LONN_y', 'TOT_UTG_x', 'TOT_UTG_y'])

df_final[['TOT_UTG', 'LONN']] = df_final[['TOT_UTG', 'LONN']].astype(str).replace("0.0", None).astype(float)

df_final.TOT_UTG.sum()

df_final[df_final['ORGNR_FRTK'] == "883971752"]

df_final = df_final.groupby(['ORGNR_FRTK', 'TJENESTE_KODE', 'FORETAKSTYPE', "HELSEREGION", "ORGNR_STATBANK"]).sum().reset_index()

sql_str = hjfunk.lag_sql_str(df_final.ORGNR_FRTK.unique())

sporring_for = f"""
    SELECT ORGNR, NAVN, RECORD_ED, FORETAKS_NR_GDATO
    FROM DSBBASE.SSB_FORETAK
    WHERE
        ORGNR IN {sql_str} AND
        EXTRACT(YEAR FROM RECORD_ED) >= {aar4} AND
        EXTRACT(YEAR FROM FORETAKS_NR_GDATO) <= {aar4}
"""

foretak_navn = hjfunk.les_sql(sporring_for, conn).rename(columns={'ORGNR': 'ORGNR_FRTK'})



df_final = pd.merge(
    df_final,
    foretak_navn,
    on='ORGNR_FRTK',
    how='left'
).copy()

tomt_navn = df_final.NAVN.isna()
df_final.loc[tomt_navn, "NAVN"] = (
    df_final.loc[tomt_navn, "FORETAKSTYPE"]
    + " "
    + df_final.loc[tomt_navn, "TJENESTE_KODE"]
    + " "
    + df_final.loc[tomt_navn, "HELSEREGION"]
)

df_final = df_final[
    [
        "ORGNR_FRTK",
        'NAVN',
        "TJENESTE_KODE",
        "FORETAKSTYPE",
        "HELSEREGION",
        "ORGNR_STATBANK",
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
]

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

df_final['SNITTLONN_kr'] = df_final['LONN'] / df_final['AARSVERK'] * 1000

df_final['sum_verdier_rad'] = round(df_final[verdi_kol].apply(abs).sum(axis=1), 1)



df_final = df_final.sort_values(['ORGNR_FRTK', 'TJENESTE_KODE', 'HELSEREGION', 'FORETAKSTYPE'])



# +
def rapport_missing(df, kol):
    missing = df[kol].isna().sum()
    tom = (df[kol] == '').sum()
    print(f"[{missing + tom}]\tAntall med missing eller tom på {kol}")

def rapport_dublett(df, kols):
    dups = df[kols].duplicated().sum()
    print(f"[{dups}]\tAntall foretak med dublett på {kols}")


# -

rapport_missing(df_final, "NAVN")
rapport_missing(df_final, "ORGNR_FRTK")
rapport_missing(df_final, "TJENESTE_KODE")
rapport_missing(df_final, "HELSEREGION")
rapport_missing(df_final, "ORGNR_STATBANK")
print(100*"-")
rapport_dublett(df_final, ["ORGNR_FRTK", "TJENESTE_KODE"])
rapport_dublett(df_final, ["NAVN", "TJENESTE_KODE"])
rapport_dublett(df_final, ["NAVN", "ORGNR_FRTK", "TJENESTE_KODE", "HELSEREGION", "ORGNR_STATBANK"])



excel_ark = {
    'Populasjonsanalyse': df_final
}

if til_lagring:
    idag = str(pd.Timestamp.today()).split(" ")[0].replace("-", "")
    sti = f"/ssb/stamme01/fylkhels/speshelse/felles/populasjon/populasjonsanalyse_{idag}.xlsx"
    lagre_excel(excel_ark, sti)


