# -*- coding: utf-8 -*-
# # Prep

# ## Pakker og tilganger

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


# ## Innlasting av filer

sti_akt   = f"/ssb/stamme01/fylkhels/speshelse/aktivitet/{aar4}/masterfil/aktivitet_masterfil_{aar4}.parquet"
sti_rgn0x = f"/ssb/stamme01/fylkhels/speshelse/regnskap/{aar4}/masterfil/helse0x_masterfil_{aar4}.parquet"
sti_rgn39 = f"/ssb/stamme01/fylkhels/speshelse/regnskap/{aar4}/masterfil/helse39_masterfil_{aar4}.parquet"
sti_per   = f"/ssb/stamme01/fylkhels/speshelse/personell/{aar4}/personell/masterfil/personell_masterfil_{aar4}.parquet"

akt = pd.read_parquet(sti_akt)
rgn0x = pd.read_parquet(sti_rgn0x)
rgn39 = pd.read_parquet(sti_rgn39)
per = pd.read_parquet(sti_per)

# ----

akt = (
    akt
    .rename(columns={'ORGNR': 'ORGNR_FRTK'})
    .drop(columns=['NAVN', 'HELSEREGION', 'RHF', 'REGION', 'ORGNR_REGION'])
)

rapport_df(akt)

# ---

# Merknader:
# - LAB, PTR, PBF, ADM finnes ikke de andre filene. -> tas ut
# - ADM i regnskapsfilene er kun data for RHFene
#     - I personellfilen er alle årsverk som ikke er pasientrettet blir regnet som ADM
#     
# Funn:
# - Tre like kolonner: ORGNR_DELREG, ORG_NR og ORGNR

rgn0x[rgn0x['ORGNR_DELREG'] != rgn0x['ORGNR']]

rgn0x.columns

rgn0x[[kol for kol in rgn0x if kol.__contains__("ORG")]].sample(4)

rgn0x = (
    rgn0x[['ORGNR', 'funksjon', 'TOT_UTG']]
    .rename(columns={'ORGNR': 'ORGNR_FRTK'})
)

rapport_df(rgn0x)

# ---

# Merknad (skjema39)
# - Hvis `SN07_1 == 86.107`, skal `funksjon`/`TJENESTE` være `REH`
# - Gjør en groupby på ORGNR_FORETAK og TJENESTE. Summer TOT_UTG
#
# Funn:
# - Noen 'ORGNR_REGION' == 'H06'

rgn39[[kol for kol in rgn39 if kol.__contains__("ORG")]].sample(4)

rgn39

rgn39 = (
    rgn39[['ORGNR', 'funksjon', 'TOT_UTG']]
    .rename(columns={'ORGNR': 'ORGNR_VIRK', 'funksjon': 'TJENESTE', 'TOT_UTG': 'REGNSKAP_TOT_UTG'})
)

rapport_df(rgn39)

# ---

# Merknad personell
# - 

per = (
    per[['ORGNR_FORETAK', 'TJENESTE', 'AARSVERK']]
    .rename(columns={'ORGNR_FORETAK': 'ORGNR_FRTK'})
    .groupby(['ORGNR_FRTK', 'TJENESTE']).sum()
    .reset_index()
)

per = per

rapport_df(per)

master_join = pd.merge(
    akt,
    rgn0x,
    how='outer',
    on=''
)













# +
def lag_sql_str(arr):
    s = "("
    for nr in arr:
        s += "'" + str(nr) + "',"
    s = s[:-1] + ")"
    return s

def les_sql(sql_spørring, tilkobling):
    """
    Utfører en SQL-spørring og returnerer en DataFrame hvor kolonnenavnene er i store bokstaver.
    
    Parametere:
    - sql_spørring (str): SQL-spørringen som skal utføres.
    - tilkobling (SQLAlchemy connection): Databaseforbindelsen som skal brukes for spørringen.
    
    Returnerer:
    - DataFrame: Resultatet av SQL-spørringen med kolonnenavnene i store bokstaver.
    """
    from sqlalchemy import text
    # Utfør SQL-spørringen
    df = pd.read_sql_query(text(sql_spørring), tilkobling)

    # Konverter kolonnenavnene til store bokstaver
    df.columns = [col.upper() for col in df.columns]

    return df


# -

len(orgnr_foretak_liste)

# +
sql_str = lag_sql_str(orgnr_foretak_liste)

sporring_for = f"""
    SELECT FORETAKS_NR, ORGNR, NAVN
    FROM DSBBASE.SSB_FORETAK
    WHERE STATUSKODE = 'B' AND ORGNR IN {sql_str}
"""
vof_for = les_sql(sporring_for, conn)
# -

vof_for = vof_for.rename(columns={'ORGNR': 'ORGNR_FRTK', 'NAVN': 'NAVN_FRTK'})



sporring_bed = f"""
    SELECT FORETAKS_NR, ORGNR, NAVN, KARAKTERISTIKK, SN07_1, SB_TYPE
    FROM DSBBASE.SSB_BEDRIFT
    WHERE STATUSKODE = 'B' AND ORGNR IN {sql_str}
"""
vof_bdr = les_sql(sporring_bed, conn)

vof_bdr = vof_bdr.rename(columns={'ORGNR': 'ORGNR_VIRK', 'NAVN': 'NAVN_VIRK'})





koblede_enheter = vof_for_orgnr_un + vof_bdr_orgnr_un

set(orgnr_foretak_liste) - set(koblede_enheter)

# Enheter jeg ikke får koblet med VoF:
# - FALCK NORGE AS AVD OSLO GRENSEVEIEN 82 med organisasjonsnummer 918 997 202 ble slettet 03.10.2023
# - ALERIS ÅLESUND med organisasjonsnummer 919 729 333 ble slettet 28.03.2023
# - NORSK ARBEIDSHELSE AS AVD SANDEFJORD med organisasjonsnummer 923 568 662 ble slettet 10.01.2023
# - NORSK ARBEIDSHELSE AS AVD FREDRIKSTAD med organisasjonsnummer 923 569 022 ble slettet 10.01.2023
# - LEGEVAKTEN med organisasjonsnummer 984 293 518 ble slettet 15.01.2013
# - ALFA KURS OG BEHANDLINGSSENTER AS med organisasjonsnummer 997 764 773 ble slettet 06.07.2023
# - '999999994' - Ingen opplysninger
# - '999999995' - Ingen opplysninger
#









# +
# fornummer = pd.Series(vof_for["FORETAKS_NR"]).array

# sporring_bed = f"""
#     SELECT FORETAKS_NR, ORGNR, NAVN, KARAKTERISTIKK, SN07_1, SB_TYPE
#     FROM DSBBASE.SSB_BEDRIFT
#     WHERE STATUSKODE = 'B' AND FORETAKS_NR IN {sql_str}
# """
# vof_bdr = les_sql(sporring_bed, conn)
# -

# Henter organisasjons- og foretaksnummer fra Virksomhets- og foretaksregisteret (VoF) og samler disse i én tabell kalt ```vof```

# +
vof_for = vof_for.rename(columns={"NAVN": "NAVN_FORETAK",
                                  "ORGNR": "ORGNR_FORETAK"})

vof_bdr = vof_bdr.rename(columns={"ORGNR": "ORGNR_BEDRIFT"})
vof_bdr["KARAKTERISTIKK"] = vof_bdr["KARAKTERISTIKK"].fillna("")
vof_bdr["NAVN_BEDRIFT"] = vof_bdr["NAVN"] + " " + vof_bdr["KARAKTERISTIKK"]

vof_bdr = vof_bdr.drop(columns=["NAVN", "KARAKTERISTIKK"])

vof = pd.merge(vof_bdr, vof_for, how="left", on="FORETAKS_NR")
vof = vof.drop(columns=["FORETAKS_NR"])

rapporteringsenheter["ORGNR_FORETAK"] = rapporteringsenheter["ORGNR_FORETAK"].apply(str)
rapporteringsenheter_vof = pd.merge(
    vof, rapporteringsenheter, how="left", on="ORGNR_FORETAK"
)
# -

# # Koble masterfiler med 24xx

akt

rgn39

per.groupby("TJENESTE").sum(numeric_only=True)







































# # Aktivitet: masterfil

# Spørsmål:
# - Det er 18 virksomheter uten ORGNR og NAVN.
# - Dubletter på ORGNR

rapporteringsvar_akt = ["UTSKRIVNINGER", "OPPHOLDSDOGN", "OPPHOLDSDAGER", "POLIKLINIKK", "DOGNPLASSER", "SENGEDOGN"]

akt

akt_gruppering = akt[['ORGNR_FORETAK'] + rapporteringsvar_akt].groupby(['ORGNR_FORETAK']).sum().reset_index().copy()

akt_gruppering['sum'] = akt_gruppering.sum(axis=1, numeric_only=True)

akt_gruppering

print("ORGNR som ikke har rapportert noe i 2022:")
akt_gruppering[akt_gruppering['sum'] == 0]['ORGNR_FORETAK'].to_numpy()

# Funn:
# - 922569738 SNUOM PSYKISK HELSE AS
# - 941455077 MEDI 3 AS
# - 971427396 STIFTELSEN NORDRE AASEN
# - 976724895 PTØ NORGE


