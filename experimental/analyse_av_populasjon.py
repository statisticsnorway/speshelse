# -*- coding: utf-8 -*-
# +
import pandas as pd
import cx_Oracle
from db1p import query_db1p
import getpass
import datetime as dt
import requests

til_lagring = False # Sett til True, hvis du skal gjøre endringer i Databasen

# +
# conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")
# -

aar4 = '2022'
aar2 = aar4[-2:]

sti_akt   = f"/ssb/stamme01/fylkhels/speshelse/aktivitet/{aar4}/masterfil/aktivitet_masterfil_{aar4}.parquet"
sti_rgn0x = f"/ssb/stamme01/fylkhels/speshelse/regnskap/{aar4}/masterfil/helse0x_masterfil_{aar4}.parquet"
sti_rgn39 = f"/ssb/stamme01/fylkhels/speshelse/regnskap/{aar4}/masterfil/helse39_masterfil_{aar4}.parquet"
sti_per   = f"/ssb/stamme01/fylkhels/speshelse/personell/{aar4}/personell/masterfil/personell_masterfil_{aar4}.parquet"

akt = pd.read_parquet(sti_akt)
# rgn0x = pd.read_parquet(sti_rgn0x)
rgn39 = pd.read_parquet(sti_rgn39)
per = pd.read_parquet(sti_per)

# # Prep

akt = (
    akt[akt['FORETAKSTYPE']
        .isin(['Privat', 'Oppdrag'])]
    .rename(columns={'ORGNR': 'ORGNR_FORETAK'})
    .drop(columns=['NAVN', 'HELSEREGION', 'RHF', 'REGION', 'ORGNR_REGION'])
)

rgn39 = (
    rgn39[['ORGNR', 'funksjon', 'TOT_UTG']]
    .rename(columns={'ORGNR': 'ORGNR_FORETAK', 'funksjon': 'TJENESTE', 'TOT_UTG': 'REGNSKAP_TOT_UTG'})
)

per = per[per['FORETAKSTYPE'].isin(['Privat', 'Oppdrag'])][['ORGNR_FORETAK', 'FRTK_NAVN', 'TJENESTE', 'AARSVERK']]

per = per.groupby(['ORGNR_FORETAK', 'FRTK_NAVN', 'TJENESTE']).sum().reset_index()



# # Koble masterfiler med 24xx











































# # Aktivitet: masterfil

# Spørsmål:
# - Det er 18 virksomheter uten ORGNR og NAVN.
# - Dubletter på ORGNR

rapporteringsvar_akt = ["UTSKRIVNINGER", "OPPHOLDSDOGN", "OPPHOLDSDAGER", "POLIKLINIKK", "DOGNPLASSER", "SENGEDOGN"]

akt[['ORGNR'] + rapporteringsvar_akt]

akt_gruppering = akt[['ORGNR'] + rapporteringsvar_akt].groupby(['ORGNR']).sum().reset_index().copy()

akt_gruppering['sum'] = akt_gruppering.sum(axis=1, numeric_only=True)

akt_gruppering

print("ORGNR som ikke har rapportert noe i 2022:")
akt_gruppering[akt_gruppering['sum'] == 0]['ORGNR'].to_numpy()

# Funn:
# - 922569738 SNUOM PSYKISK HELSE AS
# - 941455077 MEDI 3 AS
# - 971427396 STIFTELSEN NORDRE AASEN
# - 976724895 PTØ NORGE
