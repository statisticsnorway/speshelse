# +
import pandas as pd

from sqlalchemy import create_engine

import getpass
import os
# -

from fagfunksjoner import ProjectRoot

with ProjectRoot():
    import Droplister.hjelpefunksjoner as hjfunk

from itables import show

# +
aar4 = 2024
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

username = getpass.getuser()
dsn = "DB1P"
try:
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")
except:
    print("Passord ikke skrevet inn")
    password = getpass.getpass(prompt='Oracle-passord: ')
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")

# Opprett en tilkobling fra motoren
conn = engine.connect()



sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
""" 
SFU_enhet = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_enhet.shape[0]}\nKolonner: {SFU_enhet.shape[1]}")


# Fjerner enheter uten ORGNR
print(SFU_enhet['orgnr'].isna().sum())
SFU_enhet = SFU_enhet[SFU_enhet['orgnr'].notnull()]
print(SFU_enhet['orgnr'].isna().sum())

SFU_enhet.columns = [x.upper() for x in SFU_enhet.columns]

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_skjema = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_skjema.shape[0]}\nKolonner: {SFU_skjema.shape[1]}")


SFU_skjema.columns = [x.upper() for x in SFU_skjema.columns]

SFU = pd.merge(
    SFU_skjema,
    SFU_enhet,
    how='left',
    on='IDENT_NR',
    suffixes=("_skj","_enh"),
    indicator="_kobling"
)

SFU._kobling.value_counts()

SFU['NAVN_NY'] = ""

for navn_kol in ["NAVN1", "NAVN2", "NAVN3", "NAVN4", "NAVN5"]:
    SFU['NAVN_NY'] += SFU[navn_kol].fillna("") + " "

SFU['NAVN_NY'] = SFU['NAVN_NY'].str.strip()

SFU[['SKJEMA_TYPE_skj']].value_counts()

skjemaer_som_ikke_skal_ha_påminnelse_februar = ['HELSE0X', 'HELSE0Y', 'HELSE40', 'RA-0595']

SFU = SFU[~SFU["SKJEMA_TYPE_skj"].isin(skjemaer_som_ikke_skal_ha_påminnelse_februar)]

SFU_grouped = (SFU
    .groupby(["ORGNR_FORETAK", "ORGNR", "NAVN_NY"])
    .agg(
        unike_skjema=("SKJEMA_TYPE_skj", "unique"),
        ant_skjema_som_skal_leveres=("SKJEMA_TYPE_skj", "nunique"),
        ant_levert=("KVITT_TYPE_skj", "count"),
    )
)
SFU_grouped["leveringsprosent"] = (
    SFU_grouped["ant_levert"] / SFU_grouped["ant_skjema_som_skal_leveres"] * 100
)

SFU_grouped.leveringsprosent.value_counts()

påminnelse = SFU_grouped[SFU_grouped['leveringsprosent'] < 100].sort_values('leveringsprosent', ascending=False).copy()

cols = (
    ["IDENT_NR", "NAVN_NY"]
    + [col for col in SFU if "ORGNR" in col]
    + [col for col in SFU if "H_VAR" in col]
    + [col for col in SFU if "TYPE" in col]
)
SFU2 = SFU[cols].copy()

# +
sporring = """
SELECT *
FROM all_tables

ORDER BY table_name ASC
"""

tabeller = hjfunk.les_sql(sporring, conn)
tabellnavn = (
    tabeller
    [tabeller['TABLESPACE_NAME'] == "DSBBASE"]
    .TABLE_NAME.unique()
)
# -

# `tabeller` inneholder alle tabellene i Oracle. Den er som et oppslagsverk for å finne hvor hvordan man skrive SQL-spørringer. Filtrerer ut tabellene i tråd med navnsystemet beskrevet over.

mask_kostra = tabeller['OWNER'] == "KOSTRA_EXP"
mask_helse = tabeller['TABLE_NAME'].str.startswith("HELSE")
mask_årgang = tabeller['TABLE_NAME'].str.endswith(str(aar_før4))
skjemaer = tabeller[mask_kostra & mask_helse & mask_årgang].TABLE_NAME.unique()

skjemaer

# Velger ut de kolonnene jeg ønsker å ha med videre, men ser at kolonnenavnene ikke er likt på tvers av skjemaene. Har dermed manuelt gått gjennom tabellene og sett hva de heter. Legger til slutt en dictionary med oversikt over hvilke skjemaer som har hvilke kolonnenavn.

utkols_o = ["FORETAKETS_ORGNR", "FORETAKETS_NAVN", "HELSEREGION_UTFYLT",
            "HELSEREGION_EPOST", "SKJEMA_TYPE", "AARGANG"]

# Mangler FORETAKETS_NAVN i registeret (Vurdere å kople på). Setter inn 
# tom variabel KVARTAL og sletter denne senere
utkols_0X0Y = ["ORG_NR", "KVARTAL", "SKJEMA_UTFYLT",
               "EPOSTADR", "SKJEMA_TYPE",
               "AARGANG"]

utkols_48 = ['FORETAKETS_ORGNR',
             'FORETAKETS_NAVN',
             'HELSEREGION_KONTAKTPERSON',
             'HELSEREGION_EPOST',
             'SKJEMA_TYPE',
             'AARGANG']

utkols_46P = ['FORETAK_ORGNR',
              'FORETAK_NAVN',
              'HELSEREGION_UTFYLT',
              'HELSEREGION_EPOST',
              'SKJEMA_TYPE',
              'AARGANG']

skj_kol_dict = {
    'HELSE0X':  utkols_0X0Y,
    'HELSE0Y':  utkols_0X0Y,
    'HELSE38O': utkols_o,
    'HELSE38P': utkols_o,
    'HELSE39':  utkols_o,
    'HELSE40':  utkols_o,
    'HELSE41':  utkols_o,
    'HELSE44O': utkols_o,
    'HELSE44P': utkols_o,
    'HELSE45O': utkols_o,
    'HELSE45P': utkols_o,
    'HELSE46O': utkols_o,
    'HELSE46P': utkols_46P,
    'HELSE46':  utkols_o,
    'HELSE47':  utkols_o,
    'HELSE48':  utkols_48,
}


def hent_skjema(s):
    """
    Funksjon som henter inn alle variable fra angitt tabell s.
    """

    sporring = f"""
    SELECT *
    FROM KOSTRA_EXP.{s}
    """
    df = hjfunk.les_sql(sporring, conn)
    return df


# tar bort årgang fra tabellnavnet
skj_dict = {}
for skj in skjemaer:
    df = hent_skjema(skj)
    s = skj.replace("_" + str(aar_før4), "")
    skj_dict[s] = df

# lager en tom dataframe og legger suksessivt til info til en stor mastertabell
kontakt_df = pd.DataFrame()

for skj in skj_dict:
    kolonner_temp = skj_kol_dict[skj]
    df_temp = skj_dict[skj][kolonner_temp].copy()

    if skj == "HELSE0X" or skj == "HELSE0Y":
        # brukte denne variabelen som dummy-variabel, fordi navn på foretak ikke
        # var med i denne tabellen
        df_temp['KVARTAL'] = ""

    # Har nå forsikret meg om at kolonnerekkefølgen er lik for alle tabeller
    # og gjør navneendring basert på rekkefølge
    df_temp.columns = ['ORGNR_FORETAK', 'NAVN_FORETAK', 'KONTAKTPERSON', 'EPOSTADR', 'SKJEMA_TYPE', 'AARGANG']

    kontakt_df = pd.concat([kontakt_df, df_temp])


kontakt_df = kontakt_df.drop_duplicates()

kontakt_df.ORGNR_FORETAK = pd.to_numeric(kontakt_df.ORGNR_FORETAK).astype('str').str.replace(".0", "")

SFU3 = SFU2[SFU2['KVITT_TYPE_skj'].isna()].copy()[['ORGNR', 'ORGNR_FORETAK', 'NAVN_NY', 'SKJEMA_TYPE_skj', 'KVITT_TYPE_skj']]



påminnelse_df = pd.merge(
    SFU3,
    kontakt_df,
    left_on=['ORGNR_FORETAK', 'SKJEMA_TYPE_skj'],
    right_on=['ORGNR_FORETAK', 'SKJEMA_TYPE'],
    how='left',
    indicator=True
)

påminnelse_df[påminnelse_df['_merge'] == 'left_only']

påminnelse_df['EPOSTADR_lower'] = påminnelse_df['EPOSTADR'].str.lower()

påminnelse_grouped_epost = påminnelse_df.groupby(['EPOSTADR_lower', 'KONTAKTPERSON', 'ORGNR_FORETAK']).agg(
    SKJEMA_TYPER=('SKJEMA_TYPE', 'unique'),
    ORGNR=('ORGNR', 'unique'), 
    NAVN=('NAVN_NY', 'unique')
)

påminnelse_grouped_epost = påminnelse_grouped_epost.reset_index()

len(påminnelse_grouped_epost)


def helse_to_skjema(s: str):
    return s.replace("HELSE", "")


påminnelse_grouped_epost["SKJEMA_TYPER"] = (
    påminnelse_grouped_epost["SKJEMA_TYPER"]
    .apply(lambda x: (", ").join(x))
    .apply(helse_to_skjema)
)
påminnelse_grouped_epost["ORGNR"] = påminnelse_grouped_epost["ORGNR"].apply(
    lambda x: (", ").join(x)
)
påminnelse_grouped_epost["NAVN"] = påminnelse_grouped_epost["NAVN"].apply(
    lambda x: (", ").join(x)
)

påminnelse_grouped_epost


def korrekt_navn(navnestreng: str) -> str:
    # Del opp strengen på mellomrom
    ordliste = navnestreng.split()
    ny_ordliste = []

    for ord in ordliste:
        # Hvis ordet inneholder bindestrek, splitt på den og kapitaliser hver del
        if '-' in ord:
            deler = ord.split('-')
            nye_deler = [
                delord[0].upper() + delord[1:].lower() if delord else ""
                for delord in deler
            ]
            nytt_ord = '-'.join(nye_deler)
        else:
            nytt_ord = ord[0].upper() + ord[1:].lower() if ord else ""
        ny_ordliste.append(nytt_ord)

    return " ".join(ny_ordliste)


påminnelse_grouped_epost["KONTAKTPERSON"] = (
    påminnelse_grouped_epost["KONTAKTPERSON"].str.lower().apply(korrekt_navn)
)

# +
from pathlib import Path

filepath = Path.cwd().parent.parent / "data_temp" / "påminnelse.xlsx"
påminnelse_grouped_epost.to_excel(filepath, index=False)
# -















mangler_epost_list = påminnelse_df[påminnelse_df['_merge'] == 'left_only']['ORGNR_FORETAK'].to_list()

mangler_df = SFU[SFU['ORGNR_FORETAK'].isin(mangler_epost_list)].copy()

mangler_df[['ORGNR_FORETAK', 'ORGNR', 'NAVN_NY'] + [col for col in mangler_df.columns if 'POST' in col]]









SFU_enhet[SFU_enhet['NAVN'].str.contains("HALL")][['NAVN', 'ORGNR', 'ORGNR_FORETAK']]

SFU_skjema
