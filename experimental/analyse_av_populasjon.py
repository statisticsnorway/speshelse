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

til_lagring = False # Sett til True, hvis du skal gjøre endringer i Databasen
# -

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


# # Innlasting av filer

sti_akt   = f"/ssb/stamme01/fylkhels/speshelse/aktivitet/{aar4}/masterfil/aktivitet_masterfil_{aar4}.parquet"
sti_rgn0x = f"/ssb/stamme01/fylkhels/speshelse/regnskap/{aar4}/masterfil/helse0x_masterfil_{aar4}.parquet"
sti_rgn39 = f"/ssb/stamme01/fylkhels/speshelse/regnskap/{aar4}/masterfil/helse39_masterfil_{aar4}.parquet"
sti_per   = f"/ssb/stamme01/fylkhels/speshelse/personell/{aar4}/personell/masterfil/personell_masterfil_{aar4}.parquet"

akt = pd.read_parquet(sti_akt)
rgn0x = pd.read_parquet(sti_rgn0x)
rgn39 = pd.read_parquet(sti_rgn39)
per = pd.read_parquet(sti_per)

# ----

# # Bearbeiding av filer

HF, RHF, phob, rfss, rfss2, rfss3, rapporteringsenheter = hjfunk.hent_enheter_fra_klass(
    aar4
)

rfs = pd.concat([rfss, rfss2, rfss3], ignore_index=True)

rfs['FORETAKSTYPE'] = 'Støtteforetak'

HF['FORETAKSTYPE'] = 'HF'

phob['FORETAKSTYPE'] = 'Oppdrag'

RHF['FORETAKSTYPE'] = 'RHF'

foretakstyper = pd.concat([RHF, HF, phob, rfs], ignore_index=True).drop(columns=['HELSEREGION', 'NAVN_KLASS']).rename(columns={'ORGNR_FORETAK': 'ORGNR_FRTK'})

# ## Aktivitet

akt = (
    akt
    .rename(columns={'ORGNR': 'ORGNR_FRTK'})
    .drop(columns=['NAVN', 'HELSEREGION', 'RHF', 'REGION', 'ORGNR_REGION'])
)

akt['FORETAKSTYPE'] = akt['FORETAKSTYPE'].str.upper()

rapport_df(akt)

# ## Regnskap skjema0X

# Merknader:
# - LAB, PTR, PBF, ADM finnes ikke de andre filene. -> tas ut
# - ADM i regnskapsfilene er kun data for RHFene
#     - I personellfilen er alle årsverk som ikke er pasientrettet blir regnet som ADM
#     
# Funn:
# - Tre like kolonner: ORGNR_DELREG, ORG_NR og ORGNR

# +
# LAB, PTR, PBF, ADM finnes ikke de andre filene. -> tas ut

tjenester_tas_ut = [
    "LAB",
    "PTR",
    "PBF",
    "ADM",
]
rgn0x = rgn0x[~rgn0x['funksjon'].isin(tjenester_tas_ut)].copy()
# -

rgn0x = rgn0x[["ORGNR", "funksjon", "TOT_UTG"]].rename(
    columns={"ORGNR": "ORGNR_FRTK", "funksjon": "TJENESTE", "TOT_UTG": "TOT_UTG_0X"}
)

rgn0x = rgn0x.groupby(['ORGNR_FRTK', 'TJENESTE']).sum().reset_index()

rgn0x = pd.merge(
    rgn0x,
    foretakstyper,
    on='ORGNR_FRTK',
    how='left'
)

rapport_df(rgn0x)

# ---

# ## Regnskap skjema39

# Merknad (skjema39)
# - Hvis `SN07_1 == 86.107`, skal `funksjon`/`TJENESTE` være `REH`
# - Gjør en groupby på ORGNR_FORETAK og funksjon. Summer TOT_UTG
#
# Funn:
# - Noen 'ORGNR_REGION' == 'H06'

# Hvis `SN07_1 == 86.107`, skal `funksjon`/`TJENESTE` være `REH`
m_86107 = rgn39['SN07_1'] == '86.107'
rgn39.loc[m_86107, 'funksjon'] = "REH"

# ### Henter ORGNR_FRTK fra VOF
# (106 missing)

sql_str = hjfunk.lag_sql_str(rgn39[rgn39.ORGNR_FORETAK.isna()].ORGNR.unique())
sporring_bed = f"""
    SELECT FORETAKS_NR, ORGNR
    FROM DSBBASE.SSB_BEDRIFT
    WHERE STATUSKODE = 'B' AND ORGNR IN {sql_str}
"""
vof_bdr = hjfunk.les_sql(sporring_bed, conn).rename(columns={'ORGNR': 'ORGNR_VIRK'})

# +
sql_str = hjfunk.lag_sql_str(vof_bdr.FORETAKS_NR.to_list())

sporring_for = f"""
    SELECT FORETAKS_NR, ORGNR
    FROM DSBBASE.SSB_FORETAK
    WHERE STATUSKODE = 'B' AND FORETAKS_NR IN {sql_str}
"""
vof_for = hjfunk.les_sql(sporring_for, conn).rename(columns={'ORGNR': 'ORGNR_FRTK'})
# -



oppslag = pd.merge(
    vof_bdr,
    vof_for,
    on='FORETAKS_NR',
    how='left',
).drop(columns=['FORETAKS_NR'])

rgn39 = rgn39[["ORGNR", "TOT_UTG", "funksjon", "ORGNR_FORETAK", 'ID']].rename(
    columns={
        "ORGNR": "ORGNR_VIRK",
        "ORGNR_FORETAK": "ORGNR_FRTK",
        "funksjon": "TJENESTE",
        "TOT_UTG": "TOT_UTG_39",
        "ID": "FORETAKSTYPE"
    }
)

rgn39 = pd.merge(
    rgn39,
    oppslag,
    on='ORGNR_VIRK',
    how='left',
    suffixes=("_skj39", "_vof")
)

import numpy as np

rgn39["ORGNR_FRTK"] = np.where(
    rgn39["ORGNR_FRTK_skj39"].notnull(),
    rgn39["ORGNR_FRTK_skj39"],
    rgn39["ORGNR_FRTK_vof"],
)

rgn39.drop(columns=["ORGNR_FRTK_skj39", "ORGNR_FRTK_vof"], inplace=True)



rgn39 = (
    rgn39.groupby(["ORGNR_FRTK", "TJENESTE", "FORETAKSTYPE"])
    .TOT_UTG_39.sum()
    .reset_index()
    .sort_values(["ORGNR_FRTK", "TJENESTE"])
)

rgn39['FORETAKSTYPE'] = rgn39['FORETAKSTYPE'].replace("PRIVATE", "PRIVAT")

rapport_df(rgn39)

# ## Personell

per = (
    per[['ORGNR_FRTK', 'TJENESTE_KODE', 'AARSVERK', 'FORETAKSTYPE']]
    .groupby(['ORGNR_FRTK', 'TJENESTE_KODE', 'FORETAKSTYPE']).sum()
    .reset_index()
    .rename(columns={'TJENESTE_KODE': 'TJENESTE'})
)

per['FORETAKSTYPE'] = per['FORETAKSTYPE'].str.upper()

rapport_df(per)

# # Merge alle fire mastertabeller på foretak

dfs = [akt, rgn39, rgn0x, per]



import functools as ft

df_final = ft.reduce(
    lambda left, right: pd.merge(
        left, right, on=["ORGNR_FRTK", "TJENESTE", "FORETAKSTYPE"], how="outer"
    ),
    dfs,
)

df_final.columns

(df_final.ORGNR_FRTK == "").sum()

df_final = df_final.groupby(['ORGNR_FRTK', 'TJENESTE', 'FORETAKSTYPE']).sum().reset_index()

# +
sql_str = hjfunk.lag_sql_str(df_final.ORGNR_FRTK.unique())

sporring_for = f"""
    SELECT ORGNR, NAVN
    FROM DSBBASE.SSB_FORETAK
    WHERE STATUSKODE = 'B' AND ORGNR IN {sql_str}
"""
foretak_navn = hjfunk.les_sql(sporring_for, conn).rename(columns={'ORGNR': 'ORGNR_FRTK'})
# -

df_final = pd.merge(
    df_final,
    foretak_navn,
    on='ORGNR_FRTK',
    how='left'
).copy()



df_final['TOT_UTG'] = df_final['TOT_UTG_39'] + df_final['TOT_UTG_0X']

df_final = df_final[
    [
        "ORGNR_FRTK",
        'NAVN',
        "TJENESTE",
        "FORETAKSTYPE",
        "UTSKRIVNINGER",
        "OPPHOLDSDOGN",
        "OPPHOLDSDAGER",
        "POLIKLINIKK",
        "DOGNPLASSER",
        "SENGEDOGN",
        "TOT_UTG",
        "AARSVERK",
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
]



df_final['sum_verdier_rad'] = round(df_final[verdi_kol].apply(abs).sum(axis=1), 1)

pd.options.display.float_format = '{:.1f}'.format



excel_ark = {
    'Populasjonsanalyse': df_final
}

lagre_excel(excel_ark, "/ssb/stamme01/fylkhels/speshelse/felles/populasjon/populasjonsanalyse.xlsx")













# # Koble masterfiler med 24xx

# +
# sporring = f"""
#     SELECT *
#     FROM DSBBASE.DLR_ENHET_I_DELREG
#     WHERE DELREG_NR IN ('24{aar2}')
# """
# SFU_data = hjfunk.les_sql(sporring, conn)
# print(f"Rader:    {SFU_data.shape[0]}\nKolonner: {SFU_data.shape[1]}")
# SFU_data.info()


# +
# pop = SFU_data.ORGNR.copy()
# -










