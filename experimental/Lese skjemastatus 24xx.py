# # Import

import pandas as pd
import cx_Oracle
from db1p import query_db1p
import getpass
# import os
# import requests

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# +
aar4 = 2022
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

# # Vanlig SFU: `dsbbase.dlr_enhet_i_delreg `
# På enhetsnivå

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
""" 
SFU_enhet = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_enhet.shape[0]}\nKolonner: {SFU_enhet.shape[1]}")
SFU_enhet.info()


# Enheter med verdi i 'KVITT_TYPE' filtreres ut (de er nedlagte enheter). Fjerner også enheter uten ORGNR
# SFU_enhet = SFU_enhet[~SFU_enhet['KVITT_TYPE'].notnull()]
SFU_enhet = SFU_enhet[SFU_enhet['ORGNR'].notnull()]

# # Skjema-SFU: `dsbbase.dlr_enhet_i_delreg_skjema `

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_skjema = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_skjema.shape[0]}\nKolonner: {SFU_skjema.shape[1]}")
SFU_skjema.info()


SFU = pd.merge(
    SFU_skjema,
    SFU_enhet,
    how='left',
    on='IDENT_NR',
    suffixes=("_skj","_enh")
)

SFU.sample(3)

SFU[['NAVN', 'ORGNR', 'SKJEMA_TYPE_skj', 'KVITT_TYPE_skj', 'KVITT_TYPE_enh']].sample(3)

# Vi skal ha purring på de som er:
#
# - [x] ikke har `HELSE39` i `SKJEMA_TYPE` i SFU_skjema (ta bort)
# - [x] `HELSE48` **HVIS** foretak er privat. Alle andre skjema skal purres på
# - [ ] blanke på `KVITT_TYPE` i SFU_skjema
#
#

# ### Velger alle private skjema:
# > Merk at 39 er privat, men har senere leveringsfrist. Filtreres ut allerede her

# +
skjema_til_purring = ['HELSE38P', 'HELSE44P', 'HELSE45P',
                      'HELSE46P', 'HELSE47', 'HELSE48']

purring_df = SFU[SFU['SKJEMA_TYPE_skj'].isin(skjema_til_purring)]
# -

# ### Tar bort de som skal levere skjema48 og ikke er private:

maske1 = (purring_df['SKJEMA_TYPE_skj'] == 'HELSE48') & (purring_df['H_VAR2_A'] != 'OPPDRAG')
purring_df = purring_df[~maske1]

# ### Tar bort alle som ikke har noe oppført på `KVITT_TYPE_skj` 

purring_df = purring_df[purring_df['KVITT_TYPE_skj'].isna()]

purring_df[['NAVN', 'ORGNR', 'SKJEMA_TYPE_skj', 'KVITT_TYPE_skj']].sample(3)

purring_df.shape

#

foretak_til_purring = purring_df.ORGNR_FORETAK.unique()

len(foretak_til_purring)

# # Altinn-delreg 2087722

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar2}')
""" 
altinn = pd.read_sql_query(sporring, conn)
print(f"Rader:    {altinn.shape[0]}\nKolonner: {altinn.shape[1]}")
altinn.info()


foretak_til_purring_i_altinn_df = altinn[altinn.ORGNR.isin(foretak_til_purring)].ORGNR.to_numpy()

print(set(foretak_til_purring) - set(foretak_til_purring_i_altinn_df))
print(set(foretak_til_purring_i_altinn_df) - set(foretak_til_purring))
print("Hvis begge settene er tomme, er alle foretakene vi har tenkt å sende",
      "purring til, i Altinn-delregisteret.")

# Ettersom vi ønsker å sende purring til de som er innkvittert, må vi invertere listene. Dvs, at foretak i delreg `2087722` som som IKKE er i listen `foretak_til_purring`, skal innkvitteres.

# # Skrive til delregisteret

foretak_til_purring

foretak_til_innkvittering_df = altinn[~altinn['ORGNR'].isin(foretak_til_purring)]

foretak_til_innkvittering_df = (
    foretak_til_innkvittering_df
    [
        ["DELREG_NR",
         "IDENT_NR",
         "ENHETS_TYPE",
         "KVITT_TYPE",
         "KVITT_FORMAT",
         "DATO_INNKVITTERING"
         ]
    ]
)

import datetime as dt

today = dt.date.today()
formatted_date = today.strftime("%Y-%m-%d")

print("Dato i dag: ", formatted_date)

# Filtrerer ut de som allerede er inn-/utkvittert på andre måter:
na_mask = foretak_til_innkvittering_df.KVITT_TYPE.isna()
foretak_til_innkvittering_df = foretak_til_innkvittering_df[na_mask].copy()

foretak_til_innkvittering_df['KVITT_TYPE'] = 'K'
foretak_til_innkvittering_df['KVITT_FORMAT'] = 'O'
foretak_til_innkvittering_df['DATO_INNKVITTERING'] = pd.to_datetime(dt.date.today())

foretak_til_innkvittering_df

foretak_til_innkvittering_df.info()

# +
sql_ins = """INSERT INTO 
DSBBASE.DLR_KVITTER_TMP
(DELREG_NR,IDENT_NR,ENHETS_TYPE,KVITT_TYPE,KVITT_FORMAT,DATO_INNKVITTERING) 
VALUES (:1,:2,:3,:4,:5,:6)"""

# OBs, variablene i dataframe'n din må være i riktig rekkefølge ift insert

# +
# conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# Dytter data inn igjen i Oracle
# conn_t = cx_Oracle.connect(bruker + "/" + pw +"@DB1P")
# cur = conn_t.cursor()

# Prøve å laste til ORacle
rows = [tuple(x) for x in altinn.values]

# sql_ins="INSERT INTO DYNAREV.RAPP_METR_GEN_VERDI(NAVN,LABELS,VERDI) VALUES (:1,:2,:3)"
# cur.executemany(sql_ins,rows)

# conn_t.commit()

# -

# OK, Python er jeg ikke veldig god i, men jeg vet jo at det er mulig å skrive tilbake til Oracle vha SQL i Python i hvert fall. (jeg har noe kode liggende (med bruk av cx_oracle), men da må jeg lete litt…)
# Men du kan jo kanskje bruke denne sas-koden som utgangspunkt?
# Det viktigste er å koble opp mot schema DSBBASE (ikke dynarev her altså), og sette følgende variabler (obs! Dato_innkvittering må være av typen datetime (altså både dato og tid)
#
# - kvitt_type='K';
# - kvitt_format='B';
# - kvitt_undertype='D8';
# - format dato_innkvittering DATETIME7.;
# - dato_innkvittering = datetime();
#
# Du trenger også variabler fra SFU (delreg_nr, ident_nr og enhets_type) før du laster inn i tabellen.
#
# Vi kan evt ta en prat på teams om det er enklere 😊
#
# Anita
#
#
# * Eksempel på program for innkvittering av enheter i SFU ;
# LIBNAME dsbbase ORACLE user=xxx password="xxx" path=DB1P SCHEMA=DSBBASE UPDATE_LOCK_TYPE=ROW;
#
#
# * 1) Les inn orgnr som skal kvitteres til et datasett ;
#
# DATA orgnr_datasett;
#      * egen kode, evt hvor kommer orgnumrene fra? ;
# RUN;
#
# * 2) Hent informasjon om enhetene ;
#
# PROC SQL ;
#      CREATE TABLE innkvitteres AS
#      SELECT * FROM dsbbase.dlr_enhet_i_delreg
#      WHERE delreg_nr=2087722 and orgnr in
#      (select distinct orgnr from orgnr_datasett)
#      ;
# QUIT;
#
# * 3) Legg til nødvendige variabler ;
#
# data kvitter_enheter;
# set innkvitteres;
#
# skjema_type ='RA-xxx ' ; /* hvis skjema er definert, ellers fjernes dette */
# kvitt_type='K';
# kvitt_format='B';
# kvitt_undertype='D8';
# format dato_innkvittering DATETIME7.;
# dato_innkvittering = datetime();
# Keep delreg_nr ident_nr dato_innkvittering kvitt: skjema_type enhets_type; 
# run;
#
# /*proc print; Title 'Enheter som kvitteres inn'; RUN;*/
#
# * 4) Innkvitter ;
# PROC SQL;
#   INSERT INTO dsbbase.dlr_kvitter_tmp
#   (delreg_nr,ident_nr, dato_innkvittering, kvitt_type, skjema_type,enhets_type)
#   SELECT
#     delreg_nr, ident_nr, dato_innkvittering, kvitt_type, skjema_type,enhets_type
#   FROM kvitter_enheter;
# QUIT;
#
