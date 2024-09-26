# -*- coding: utf-8 -*-
# Dette scriptet ble brukt til å fjerne skjema 39 for utvalgte enheter i populasjonen til Spesialisthelsetjenesten.
#
# For å gjøre dette, slettes enheten fra `DSBBASE.DLR_ENHET_I_DELREG_SKJEMA` og endrer variabelen `SKJEMA_TYPE` i `DSBBASE.DLR_ENHET_I_DELREG` med tilhørende oppdateringsmelding og datostempel i hhv `KOMMENTAR_INT` og `DATO_UPDATE.
#
#

# +
import pandas as pd
import cx_Oracle

import getpass
import datetime as dt
import requests

# +
import os
import sys

# Legger til banen til mappen på nivået over
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath('hjelpefunksjoner'))))

# Deretter kan du importere filen
from Droplister.hjelpefunksjoner import les_sql

# -

from sqlalchemy import create_engine

username = getpass.getuser()
dsn = "DB1P"
try:
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")
    conn = engine.connect()
except:
    print("Passord ikke skrevet inn")
    password = getpass.getpass(prompt='Oracle-passord: ')
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")
    conn = engine.connect()


# ---

# ## DSBBASE.DLR_ENHET_I_DELREG

def fjern_39(tekst):
    tekst = tekst.replace(" 39", " ").replace("39 ", " ").replace("39", " ")
    tekst = " ".join(tekst.split())
    return tekst


sporring = f"""
        SELECT *
        FROM DSBBASE.DLR_ENHET_I_DELREG
        WHERE DELREG_NR IN ('2424')
"""
df = les_sql(sporring, conn)

kolonner = [
    "DELREG_NR",
    "IDENT_NR",
    "ENHETS_TYPE",
    "NAVN",
    "ORGNR",
    "ORGNR_FORETAK",
    "SKJEMA_TYPE",
    "H_VAR2_A",
    "DATO_UPDATE",
    "USER_UPDATE",
    "KOMMENTAR_INT",
]

# +
m_39 = df.SKJEMA_TYPE.fillna("").str.contains("39")
m_oppdrag = df.H_VAR2_A.fillna("").str.contains("OPPDRAG")
m_unntak = df['ORGNR_FORETAK'].isin(['965985166'])

m_skal_endres = m_39 & (~m_oppdrag | m_unntak)
# -

df2 = df[m_skal_endres].copy()

df_endre = df2[kolonner].copy()

df_endre['SKJEMA_TYPE'] = df_endre['SKJEMA_TYPE'].apply(fjern_39)

df_endre = df_endre.drop(columns=['NAVN', 'ORGNR', 'ORGNR_FORETAK', 'H_VAR2_A'])

df_endre['USER_UPDATE'] = "MFM"

df_endre['KOMMENTAR_INT'] = "Fjerner skjema 39 etter vedtak i arbeidsgruppa."

today = pd.to_datetime("today").strftime("%Y-%m-%d %H:%M:%S")

df_endre['DATO_UPDATE'] = pd.to_datetime("today")

df_endre.dtypes

df_endre.sample(3)

# ### Gjøre endringene med en UPDATE

# SQL UPDATE statement with positional parameters
sql_update = """
UPDATE DSBBASE.DLR_ENHET_I_DELREG
    SET SKJEMA_TYPE = :1, DATO_UPDATE = :2, USER_UPDATE = :3, KOMMENTAR_INT = :4
    WHERE DELREG_NR = :5 AND IDENT_NR = :6 AND ENHETS_TYPE = :7
"""

rows = [
    (
        row['SKJEMA_TYPE'],
        row['DATO_UPDATE'],
        row['USER_UPDATE'],
        row['KOMMENTAR_INT'],
        row['DELREG_NR'],
        row['IDENT_NR'],
        row['ENHETS_TYPE']
    )
    for index, row in df_endre.iterrows()
]

# +
# Assuming 'engine' is your existing SQLAlchemy engine
raw_conn = engine.raw_connection()
cur = raw_conn.cursor()

try:
    # Execute the update operation
    cur.executemany(sql_update, rows)
    # Commit the changes
    raw_conn.commit()
    print(f"Updated {cur.rowcount} rows.")
except Exception as e:
    # Rollback in case of error
    raw_conn.rollback()
    print(f"An error occurred: {e}")
finally:
    # Close the cursor and connection
    cur.close()
    raw_conn.close()
# -

# ## Slette enhetene i DSBBASE.DLR_ENHET_I_DELREG_SKJEMA

df_oppslag = df[['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE', 'NAVN', 'ORGNR', 'ORGNR_FORETAK', 'H_VAR2_A']].copy()

sporring = """
SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
    WHERE DELREG_NR = 2424
"""
df_skj = les_sql(sporring, conn)

df_skj39 = df_skj[df_skj['SKJEMA_TYPE'] == 'HELSE39'].copy()

df_skj39 = pd.merge(
    df_skj39,
    df_oppslag,
    on=['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE'],
    how='left'
)

m_priv = df_skj39['H_VAR2_A'] == 'PRIVAT'
m_klarer_seg = df_skj39['ORGNR_FORETAK'].isin(['965985166'])

til_slett = df_skj39[m_priv | m_klarer_seg].copy()

til_slett = til_slett[['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE', 'SKJEMA_TYPE']]



raw_conn = engine.raw_connection()
cur = raw_conn.cursor()

sql_delete = """
DELETE FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
WHERE DELREG_NR = :1 AND IDENT_NR = :2 AND ENHETS_TYPE = :3 AND SKJEMA_TYPE = :4
"""

rows_to_delete = [
    (
        row['DELREG_NR'],
        row['IDENT_NR'],
        row['ENHETS_TYPE'],
        row['SKJEMA_TYPE']
    )
    for index, row in til_slett.iterrows()
]

# Execute the delete operation
try:
    cur.executemany(sql_delete, rows_to_delete)
    # Commit changes
    raw_conn.commit()
except Exception as e:
    # Rollback in case of error
    raw_conn.rollback()
    print(f"An error occurred: {e}")
finally:
    # Close cursor and connection
    cur.close()
    raw_conn.close()











# ## Skjema i 24XX på overordnet nivå
#
# Slettes de herfra, slettes de totalt

sporring = """
SELECT *
    FROM DSBBASE.DLR_DELREG_SKJEMA
    WHERE DELREG_NR = 2424
"""
test = les_sql(sporring, conn)



test
