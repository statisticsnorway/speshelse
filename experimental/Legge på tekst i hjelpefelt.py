# Denne koden henter alle skjema et foretak skal levere og legger disse inn som hjelpefelt i 20877yy ('H_VAR1_A' og 'H_VAR2_A')

import pandas as pd
import cx_Oracle
from db1p import query_db1p
import getpass

import os

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# +
aar4 = 2024
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

# # Private: 208775yy

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar2}')
"""
altinn_raw = pd.read_sql_query(sporring, conn)
print(f"Rader:    {altinn_raw.shape[0]}\nKolonner: {altinn_raw.shape[1]}")

altinn = altinn_raw[['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE'] + [col for col in altinn_raw.columns if "H_V" in col]]

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU = pd.read_sql_query(sporring, conn)

skjematyper_per_foretak = SFU[(SFU['H_VAR2_A'] == 'PRIVAT')].groupby(['ORGNR_FORETAK']).SKJEMA_TYPE.unique()

skjematyper_per_foretak = skjematyper_per_foretak.reset_index()

skjematyper_per_foretak['SKJEMA_TYPE2'] = skjematyper_per_foretak.SKJEMA_TYPE.apply(lambda x: [e for e in x if e])

skjematyper_per_foretak['SKJEMA_TYPE2'] = skjematyper_per_foretak['SKJEMA_TYPE2'].apply(lambda x: " ".join(x))

m_foretak = SFU['ORGNR'] == SFU['ORGNR_FORETAK']
foretak_navn_df = SFU[m_foretak][['ORGNR_FORETAK', 'NAVN']].copy()

skjematyper_per_foretak = pd.merge(
    skjematyper_per_foretak,
    foretak_navn_df,
    on='ORGNR_FORETAK',
    how='left'
)

# Lage skjema navn utfra SKJEMA_TYPE-variabel:
skjematyper_per_foretak['SKJEMA'] = skjematyper_per_foretak['SKJEMA_TYPE2'].str.split(" ").apply(lambda x: ", HELSE".join(x))
skjematyper_per_foretak['SKJEMA'] = skjematyper_per_foretak['SKJEMA'].str.replace("1", "P")
skjematyper_per_foretak['SKJEMA'] = skjematyper_per_foretak['SKJEMA'].apply(lambda x: "HELSE" + x)
skjematyper_per_foretak['FORETAK'] = "(" + skjematyper_per_foretak['ORGNR_FORETAK'] + ") " + skjematyper_per_foretak['NAVN']
oppdater_altinn = pd.merge(
    altinn_raw[['IDENT_NR', 'DELREG_NR', 'ENHETS_TYPE', 'ORGNR', 'NAVN']],
    skjematyper_per_foretak[['ORGNR_FORETAK', 'SKJEMA', 'FORETAK']],
    left_on='ORGNR',
    right_on='ORGNR_FORETAK',
    how='outer',
    indicator=True
)





# ## Tilpasse data som skal inn i delregisteret

# +
import cx_Oracle
import pandas as pd
import getpass

# Forbered dataene
oppdater_altinn['DELREG_NR'] = f"20877{aar2}"
oppdater_altinn['DATO_INSERT'] = pd.Timestamp.now().floor('s')
oppdater_altinn['USER_INSERT'] = getpass.getuser().upper()
oppdater_altinn['DATO_UPDATE'] = pd.Timestamp.now().floor('s')
oppdater_altinn['USER_UPDATE'] = getpass.getuser().upper()

# Tilpass kolonnenavnene til databasen
oppdater_altinn = oppdater_altinn.rename(columns={'SKJEMA': 'H_VAR1_A', 'FORETAK': 'H_VAR2_A'})

# Opprett forbindelse til Oracle-databasen
# conn = cx_Oracle.connect(getpass.getuser() + "/" + getpass.getpass(prompt='Oracle-passord: ') + "@DB1P")
cur = conn.cursor()

# SQL for oppdatering
sql_update = """
UPDATE DSBBASE.DLR_ENHET_I_DELREG
SET H_VAR1_A = :H_VAR1_A,
    H_VAR2_A = :H_VAR2_A,
    USER_UPDATE = :USER_UPDATE,
    DATO_UPDATE = :DATO_UPDATE
WHERE DELREG_NR = :DELREG_NR
  AND IDENT_NR = :IDENT_NR
  AND ENHETS_TYPE = :ENHETS_TYPE
"""

# Forbered rader for oppdatering
rows_to_update = [
    {
        'H_VAR1_A': row.H_VAR1_A,
        'H_VAR2_A': row.H_VAR2_A,
        'USER_UPDATE': row.USER_UPDATE,
        'DATO_UPDATE': row.DATO_UPDATE,
        'DELREG_NR': row.DELREG_NR,
        'IDENT_NR': row.IDENT_NR,
        'ENHETS_TYPE': row.ENHETS_TYPE
    }
    for row in oppdater_altinn.itertuples(index=False)
]
# -

print(sql_update)

# Forbered rader for SQL-inserten
rows = [tuple(x) for x in oppdater_altinn.itertuples(index=False, name=None)]

# Utfør oppdateringen
try:
    if rows_to_update:
        cur.executemany(sql_update, rows_to_update)
        conn.commit()
        print(f"{len(rows_to_update)} rader ble oppdatert.")
    else:
        print("Ingen rader å oppdatere.")
except cx_Oracle.DatabaseError as e:
    error, = e.args
    print(f"Databasefeil: {error.message}")
    conn.rollback()
# finally:
#     cur.close()
#     conn.close()




# # Private med oppdrags- og bestillerdokument 25468

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('25468')
"""
phob_raw = pd.read_sql_query(sporring, conn)
print(f"Rader:    {altinn_raw.shape[0]}\nKolonner: {altinn_raw.shape[1]}")

phib = phob_raw[['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE'] + [col for col in altinn_raw.columns if "H_V" in col]]

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU = pd.read_sql_query(sporring, conn)

skjematyper_per_foretak = SFU[(SFU['H_VAR2_A'] == 'OPPDRAG')].groupby(['ORGNR_FORETAK']).SKJEMA_TYPE.unique()

skjematyper_per_foretak = skjematyper_per_foretak.reset_index()

skjematyper_per_foretak['SKJEMA_TYPE2'] = skjematyper_per_foretak.SKJEMA_TYPE.apply(lambda x: [e for e in x if e])

skjematyper_per_foretak['SKJEMA_TYPE2'] = skjematyper_per_foretak['SKJEMA_TYPE2'].apply(lambda x: " ".join(x))

m_foretak = SFU['ORGNR'] == SFU['ORGNR_FORETAK']
foretak_navn_df = SFU[m_foretak][['ORGNR_FORETAK', 'NAVN']].copy()

skjematyper_per_foretak = pd.merge(
    skjematyper_per_foretak,
    foretak_navn_df,
    on='ORGNR_FORETAK',
    how='left'
)

# Lage skjema navn utfra SKJEMA_TYPE-variabel:
skjematyper_per_foretak['SKJEMA'] = skjematyper_per_foretak['SKJEMA_TYPE2'].str.split(" ").apply(lambda x: ", HELSE".join(x))
skjematyper_per_foretak['SKJEMA'] = skjematyper_per_foretak['SKJEMA'].str.replace("1", "P")
skjematyper_per_foretak['SKJEMA'] = skjematyper_per_foretak['SKJEMA'].apply(lambda x: "HELSE" + x)
skjematyper_per_foretak['FORETAK'] = "(" + skjematyper_per_foretak['ORGNR_FORETAK'] + ") " + skjematyper_per_foretak['NAVN']
oppdater_phob = pd.merge(
    phob[['IDENT_NR', 'DELREG_NR', 'ENHETS_TYPE', 'ORGNR', 'NAVN']],
    skjematyper_per_foretak[['ORGNR_FORETAK', 'SKJEMA', 'FORETAK']],
    left_on='ORGNR',
    right_on='ORGNR_FORETAK',
    how='outer',
    indicator=True
)

oppdater_phob



# ## Tilpasse data som skal inn i delregisteret

# +
import cx_Oracle
import pandas as pd
import getpass

# Forbered dataene
oppdater_phob['DATO_INSERT'] = pd.Timestamp.now().floor('s')
oppdater_phob['USER_INSERT'] = getpass.getuser().upper()
oppdater_phob['DATO_UPDATE'] = pd.Timestamp.now().floor('s')
oppdater_phob['USER_UPDATE'] = getpass.getuser().upper()

# Tilpass kolonnenavnene til databasen
oppdater_phob = oppdater_phob.rename(columns={'SKJEMA': 'H_VAR1_A', 'FORETAK': 'H_VAR2_A'})
# -

oppdater_phob

# +
# Opprett forbindelse til Oracle-databasen
# conn = cx_Oracle.connect(getpass.getuser() + "/" + getpass.getpass(prompt='Oracle-passord: ') + "@DB1P")
cur = conn.cursor()

# SQL for oppdatering
sql_update = """
UPDATE DSBBASE.DLR_ENHET_I_DELREG
SET H_VAR1_A = :H_VAR1_A,
    H_VAR2_A = :H_VAR2_A,
    USER_UPDATE = :USER_UPDATE,
    DATO_UPDATE = :DATO_UPDATE
WHERE DELREG_NR = :DELREG_NR
  AND IDENT_NR = :IDENT_NR
  AND ENHETS_TYPE = :ENHETS_TYPE
"""

# Forbered rader for oppdatering
rows_to_update = [
    {
        'H_VAR1_A': row.H_VAR1_A,
        'H_VAR2_A': row.H_VAR2_A,
        'USER_UPDATE': row.USER_UPDATE,
        'DATO_UPDATE': row.DATO_UPDATE,
        'DELREG_NR': row.DELREG_NR,
        'IDENT_NR': row.IDENT_NR,
        'ENHETS_TYPE': row.ENHETS_TYPE
    }
    for row in oppdater_phob.itertuples(index=False)
]
# -

rows_to_update



print(sql_update)

# Forbered rader for SQL-inserten
rows = [tuple(x) for x in oppdater_phob.itertuples(index=False, name=None)]

# Utfør oppdateringen
try:
    if rows_to_update:
        cur.executemany(sql_update, rows_to_update)
        conn.commit()
        print(f"{len(rows_to_update)} rader ble oppdatert.")
    else:
        print("Ingen rader å oppdatere.")
except cx_Oracle.DatabaseError as e:
    error, = e.args
    print(f"Databasefeil: {error.message}")
    conn.rollback()
# finally:
#     cur.close()
#     conn.close()



