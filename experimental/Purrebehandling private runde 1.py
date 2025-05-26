# +
import pandas as pd

from sqlalchemy import create_engine

import getpass
import os
# -

import datetime as dt


import sys
sys.path.insert(0, '..')

import Droplister.hjelpefunksjoner as hjfunk

aar4 = 2024
aar2 = str(aar4)[-2:]

# ## Tilgang oracle

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

# # Vanlig SFU: `dsbbase.dlr_enhet_i_delreg `
# På enhetsnivå

sporring = f"""
    SELECT IDENT_NR, ENHETS_TYPE, SKJEMA_TYPE, ORGNR, ORGNR_FORETAK, NAVN1, NAVN2, NAVN3, NAVN4, NAVN5, H_VAR2_A
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
""" 
SFU_enhet = hjfunk.les_sql(sporring, conn)
print(f"Rader:    {SFU_enhet.shape[0]}\nKolonner: {SFU_enhet.shape[1]}")


# # Skjema-SFU: `dsbbase.dlr_enhet_i_delreg_skjema `

sporring = f"""
    SELECT IDENT_NR, ENHETS_TYPE, SKJEMA_TYPE, KVITT_TYPE
    FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_skjema = hjfunk.les_sql(sporring, conn)
print(f"Rader:    {SFU_skjema.shape[0]}\nKolonner: {SFU_skjema.shape[1]}")


SFU = pd.merge(
    SFU_skjema,
    SFU_enhet,
    how='left',
    on=['IDENT_NR', 'ENHETS_TYPE'],
    suffixes=("_skj","_enh"),
    indicator="_kobling"
)

# +
SFU['NAVN_NY'] = ""

for navn_kol in ["NAVN1", "NAVN2", "NAVN3", "NAVN4", "NAVN5"]:
    SFU['NAVN_NY'] += SFU[navn_kol].fillna("") + " "

SFU['NAVN_NY'] = SFU['NAVN_NY'].str.strip()
SFU = SFU.drop(columns=[col for col in SFU.columns if "NAVN" in col and col != "NAVN_NY"] + ['_kobling'])

# +
# from functions.hjelpefunksjoner import lagre_excel
# til_excel = {'Ark1': SFU}
# lagre_excel(til_excel, "/ssb/bruker/mfm/data_temp/purringV25.xlsx")
# -

# # Delregister 20877xx

# Dette er et eget delregister opprettet for kommunikasjon med de private via Altinn. Alle foretak som ikke har blitt innkvittert godkjent, vil få purring. Dvs. alle med missing på KVITT_TYPE får purring.

sporring = f"""
    SELECT DELREG_NR, IDENT_NR, ENHETS_TYPE, KVITT_TYPE, ORGNR, KVITT_FORMAT, DATO_INNKVITTERING
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar2}')
"""
altinn = hjfunk.les_sql(sporring, conn)
print(f"Rader:    {altinn.shape[0]}\nKolonner: {altinn.shape[1]}")


# +
# altinn = altinn[altinn['KVITT_TYPE'].isna()]
# -

altinn

# # Purring Februar:

# Vi skal purre på virksomheter som:
#
# - [x] ikke har `HELSE39` i `SKJEMA_TYPE` i SFU_skjema (ta bort)
# - [x] `HELSE48` **HVIS** foretak er privat. Alle andre skjema skal purres på
# - [x] blanke på `KVITT_TYPE` i SFU_skjema
#
#

# ## Velger alle private skjema:

skjema_til_purring = ['HELSE38P', 'HELSE44P', 'HELSE45P',
                      'HELSE46P', 'HELSE47', 'HELSE48']

purring_df = (
    SFU[SFU["SKJEMA_TYPE_skj"].isin(skjema_til_purring)]
    .reset_index(drop=True)
    .query("KVITT_TYPE.isna()")
)

m_oppdrag48 = (purring_df["SKJEMA_TYPE_skj"] == 'HELSE48') & (purring_df['H_VAR2_A'] == 'OPPDRAG')
purring_df = purring_df[~m_oppdrag48]

# +
print(f"Antall virksomheter i SFU som har tomme felt på KVITT_TYPE er: {purring_df.shape[0]}")

foretak_til_purring = purring_df.ORGNR_FORETAK.unique()
print(f"Disse fordeler seg på {len(foretak_til_purring)} unike foretak.")
# -

purring_df.groupby('SKJEMA_TYPE_skj').agg(
    ant_foretak=('ORGNR_FORETAK', 'nunique'),
    ant_virk=('ORGNR', 'nunique'),
).reset_index().style.set_caption("Gjenstående foretak og virksomheter per skjema")

purring_df

# ### SKJEMA 47 

df47 = purring_df[purring_df['SKJEMA_TYPE_skj'] == 'HELSE47'].copy()

aar2_for = int(aar2) -1

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
    WHERE DELREG_NR IN ('24{aar2_for}')
"""
kontakt = hjfunk.les_sql(sporring, conn)


df47 = pd.merge(
    kontakt,
    df47,
    on=['IDENT_NR', 'ENHETS_TYPE']
)

kontakt[kontakt['KONTAKTPERSON_EPOST'].notna()]

df47

# ---

purring_df2 = purring_df.groupby('ORGNR_FORETAK').agg(
    SKJEMA_TYPER=('SKJEMA_TYPE_skj', 'unique'),
    ORGNR=('ORGNR', 'unique'),
    NAVN=('NAVN_NY', 'unique')
).reset_index()

df = pd.merge(
    altinn,
    purring_df2,
    left_on=['ORGNR'],
    right_on=['ORGNR_FORETAK'],
    how='outer',
    suffixes=('_altinn', '_SFU'),
    indicator=True
)
df['_merge'] = df['_merge'].map({'left_only': 'skal_kvitteres_K', 'both': 'skal_ha_purring', 'right_only': 'ikke_private'}).astype(str)

assert ((df['KVITT_TYPE'] != 'K') & (df['_merge'] == 'skal_kvitteres_K')).sum() == 0, "Det er nye enheter som skal utkvitteres"

# # Innkvitteres `KVITT_TYPE='K'`

# +
foretak_til_innkvittering_df = altinn[(~altinn['ORGNR'].isin(foretak_til_purring)) & (altinn['KVITT_TYPE'].isna())]  # de som ikke skal ha purring, innkvitteres

foretak_til_innkvittering_df = (
    foretak_til_innkvittering_df
    [
        [
         "DELREG_NR",
         "IDENT_NR",
         "ENHETS_TYPE",
         "KVITT_TYPE",
         "KVITT_FORMAT",
         "DATO_INNKVITTERING"
         ]
    ]
)

# Filtrerer ut de som allerede er inn-/utkvittert på andre måter:
na_mask = foretak_til_innkvittering_df.KVITT_TYPE.isna()
foretak_til_innkvittering_df = foretak_til_innkvittering_df[na_mask].copy()
# -

foretak_til_innkvittering_df

foretak_til_innkvittering_df = df[df["_merge"] == "skal_kvitteres_K"][
    [
        "DELREG_NR",
        "IDENT_NR",
        "ENHETS_TYPE",
        "KVITT_TYPE",
        "KVITT_FORMAT",
        "DATO_INNKVITTERING",
    ]
].copy()

foretak_til_innkvittering_df['DELREG_NR'] = foretak_til_innkvittering_df['DELREG_NR'].astype(int).astype(str)

foretak_til_innkvittering_df['KVITT_TYPE'] = 'K'
foretak_til_innkvittering_df['KVITT_FORMAT'] = 'O'
foretak_til_innkvittering_df['DATO_INNKVITTERING'] = pd.to_datetime(dt.date.today())

len(foretak_til_innkvittering_df)

# +
# Må skrives om! Se neste del av koden. Ikke alle feltene skal oppdateres (Ikke nøklene blant annet)
#sql_ins = (
#    "UPDATE INTO DSBBASE"
#    ".dlr_enhet_i_delreg("
#    "DELREG_NR,IDENT_NR,ENHETS_TYPE,"
#    "KVITT_TYPE,KVITT_FORMAT,DATO_INNKVITTERING"
#    ")"
#    " VALUES (:1,:2,:3,:4,:5,:6)"
#)
#
#print("Dobbeltsjekk sql-spørringen:")
#print(sql_ins)
#
#rows = [tuple(x) for x in foretak_til_innkvittering_df.values]
# -



if rows:
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.executemany(sql_ins, rows)
        conn.commit()
        print(f"Det er gjort {len(rows)} radendringer. Kontroller i SFU.")
    finally:
        conn.close()





# # Sett skjematype og virksomhetsnummer i hjelpefelt

df2 = df[df['_merge'] == 'skal_ha_purring'][['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE', 'ORGNR_FORETAK', 'SKJEMA_TYPER', 'ORGNR_SFU']].copy()

df2['DELREG_NR'] = df2['DELREG_NR'].astype(int).astype(str)

df2['SKJEMA_TYPER'] = df2['SKJEMA_TYPER'].apply(lambda x: ", ".join(x)).str.replace("HELSE", "SKJEMA ")
df2['ORGNR_SFU'] = df2['ORGNR_SFU'].apply(lambda x: ", ".join(x))

df2['H_VAR3_A'] = df2['SKJEMA_TYPER'] + ' - ' + df2['ORGNR_SFU']

# +
foretak_til_purring = df2.copy()

foretak_til_purring = (
    foretak_til_purring
    [
        [
         "H_VAR3_A",
         "DELREG_NR",
         "IDENT_NR",
         "ENHETS_TYPE",
         ]
    ]
)

# +
# Oppdateringsspørring
sql_update = (
    "UPDATE DSBBASE.DLR_ENHET_I_DELREG "
    "SET h_var3_a = :1 "
    "WHERE delreg_nr = :2 "
    "  AND ident_nr = :3 "
    "  AND enhets_type = :4"
)

print("Dobbeltsjekk sql-spørringen:")
print(sql_update)

rows = [tuple(x) for x in foretak_til_purring.values]
rows
# -

if rows:
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        cur.executemany(sql_update, rows)
        conn.commit()
        print(f"Det er gjort {len(rows)} radendringer. Kontroller i SFU.")
    except Exception as e:
        print("Feil oppstod:", e)
    finally:
        conn.close()


