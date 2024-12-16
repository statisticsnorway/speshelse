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


# ## Legge inn data i databasen

# SAS-kode:
# ```libname dsbbase oracle user=&bruker  password="&passw" path="DB1P" schema='dsbbase';
# proc sql;
# insert into dsbbase.dlr_load_enhet_tmp
#   (delreg_nr, enhets_type, orgnr)
# select
#    delreg_nr, enhets_type, orgnr
#   from dsbbase.dlr_enhet_i_delreg
# where delreg_nr = &delreg_nr. and enhets_type = 'FRTK';
# quit;
# ```

sql_ins = f"""
INSERT INTO dsbbase.dlr_load_enhet_tmp
  (DELREG_NR, ENHETS_TYPE, ORGNR)
SELECT
  DELREG_NR, ENHETS_TYPE, ORGNR
  FROM dsbbase.dlr_enhet_i_delreg
WHERE DELREG_NR = 20877{aar2} AND ENHETS_TYPE = 'FRTK'
"""

sql_ins

(", ").join(orgnr_inn)













# +
kolonner = ", ".join(til_altinn.columns)

indices = [f":{x}" for x in range(1, len(til_altinn.columns) + 1)]
indices = ", ".join(indices)
# -

sql_ins = (
    "INSERT INTO DSBBASE.DLR_ENHET_I_DELREG (" +
    kolonner +
    ") VALUES (" + 
    indices +
    ")"
)

# +
# Oppretter skrivekontakt med Oracle
cur = conn.cursor()

# Stabler om dataframen til SQL-vennlig innlesing
rows = [tuple(x) for x in foretak_til_innkvittering_df.values]

# Hvis til_lagring = True kjøres SQL-inserten
if til_lagring and len(rows) != 0:
    cur.executemany(sql_ins, rows)
    conn.commit()
    print(f"Det er gjort {len(rows)} radendringer. Kontroller i SFU.")

