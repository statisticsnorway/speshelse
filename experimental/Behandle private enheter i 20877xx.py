# # Opprette nytt delregister 20877XX
#
# - Etter kopiering av fjorårets enheter
# - Sjekker kopieringen mot de nye droplistene
# - Legger inn nye og fjerner utgåtte enheter
#
# **ADVARSEL** Ikke kjør all koden ukritisk, da koden gjør endringer i delregisterne.

# +
import pandas as pd
import cx_Oracle
from db1p import query_db1p
import getpass

from datetime import datetime
import datetime as dt
# -

import os

from glob import glob

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# +
aar4 = 2025
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

til_lagring = True

# ## Importer siste brukerliste

dropliste_sti = f"/ssb/stamme01/fylkhels/speshelse/felles/droplister/{aar4}/"

os.path.exists(dropliste_sti)

droplistemapper = os.listdir(dropliste_sti)

date_folders = [f for f in droplistemapper if f.isdigit() and len(f) == 6]
date_folders.sort(key=lambda x: datetime.strptime(x, '%d%m%y'))
siste_dropliste_dato = date_folders[-1]

print(siste_dropliste_dato)

sti = dropliste_sti + siste_dropliste_dato + "/"
filnavn = os.listdir(sti)[0]

filnavn

sti = glob(f"{dropliste_sti}/{siste_dropliste_dato}/Brukerliste*")[0]

pop = pd.read_csv(sti, encoding='latin1', sep=";", dtype="object")

pop.sample(3)

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar2}')
"""
altinn_raw = pd.read_sql_query(sporring, conn)
print(f"Rader:    {altinn_raw.shape[0]}\nKolonner: {altinn_raw.shape[1]}")

altinn = altinn_raw[['IDENT_NR', 'ORGNR', 'ORGNR_FORETAK', 'NAVN']]

# ## Nye enheter

pop.head(3)

sette_sammen = pd.merge(
    altinn,
    pop,
    on='ORGNR_FORETAK',
    how='outer',
    indicator=True
)

sette_sammen._merge.map({'both': 'både i delreg altinn og brukerliste',
                         'right_only': 'kun i brukerliste',
                         'left_only': 'kun på altinn delreg'}).value_counts()

sette_sammen["SKJEMA_TYPE"].value_counts()



nye = sette_sammen.loc[(sette_sammen['_merge'] == 'right_only') & (sette_sammen['SKJEMA_TYPE'] == '381 441 451 461 47')]

nye

ut = sette_sammen.query('_merge == "left_only"').copy()

ut

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU.shape[0]}\nKolonner: {SFU.shape[1]}")

skjematyper_per_foretak = SFU[SFU['H_VAR2_A'] == 'PRIVAT'].groupby(['ORGNR_FORETAK']).SKJEMA_TYPE.unique()

# ## Hente inn enheter i siste 20877XX

# ## Bestemme ny populasjon

# DELREGISTER, IDENTNUMMER, ENHETSTYPE
#
#
# ORGNUMMER er ikke viktig for kobling av tabeller. IDENTNUMMER kommer fra VoF. 

# NB: Sett fornuftig filter for å kune sitte igjen med de private foretakene i populasjonen:
priv_pop = pop[(pop["SKJEMA_TYPE"].str.contains("47")) & (~pop["SKJEMA_TYPE"].str.contains("0X"))].copy()

# +
sammen = pd.merge(
    priv_pop,
    altinn,
    how='outer',
    on='ORGNR_FORETAK',
    indicator=True
)

sammen._merge.value_counts()

sammen['status'] = sammen._merge.map(
    {'both': 'behold',
     'right_only': 'ut',
     'left_only': 'inn'}
)
# -

sammen[sammen["status"] != "behold"]

# # Eksperimentelt

# ## Gjøre endringer i delreg 20877XX

# Henter inn informasjon om de nye enhetene fra 24XX:

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
""" 
SFU_enhet = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_enhet.shape[0]}\nKolonner: {SFU_enhet.shape[1]}")

m1 = sammen["status"] == "behold"
m2 = sammen["status"] == "inn"
ny_altinn = sammen[m1 | m2].copy()

orgnr_inn = list(sammen[sammen['status'] == 'inn']['ORGNR_FORETAK'].unique())

sammen[sammen['status'] == 'inn']

til_altinn = SFU_enhet[(SFU_enhet['ORGNR_FORETAK'].isin(orgnr_inn)) & SFU_enhet['H_VAR2_N'].notnull()].copy()

til_altinn

# Omorganiser kun de felles kolonnene, og behold de unike kolonnene som de er
felles_kol = [col for col in altinn_raw.columns if col in til_altinn.columns]
til_altinn = til_altinn[felles_kol + [col for col in til_altinn.columns if col not in felles_kol]]

altinn_raw.columns

# Dobbeltsjekk at enhetene ikke allerede ligger i delregisteret
assert len(altinn_raw[altinn_raw.ORGNR_FORETAK.isin(orgnr_inn)]) == 0, "Enheter ligger allerede i systemet"


def tile_df(df, num_cols, num_rows):
    n = len(df.columns)
    num_rows = min(num_rows, len(df))

    for i in range(0, n, num_cols):
        if i + num_cols < n:
            display(df.iloc[:, i:i + num_cols].sample(num_rows))
        else:
            display(df.iloc[:, i:].sample(num_rows))


# ## Tilpasse data som skal inn i delregisteret

til_altinn

til_altinn['DELREG_NR'] = f"20877{aar2}"
til_altinn['DATO_INSERT'] = pd.Timestamp.now().floor('s')
til_altinn['USER_INSERT'] = getpass.getuser().upper()
til_altinn['DATO_UPDATE'] = None
til_altinn['USER_UPDATE'] = None

til_altinn[["DELREG_NR", "DATO_INSERT", "USER_INSERT", "DATO_UPDATE", "USER_UPDATE"]]

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

# +
kolonner = ", ".join(til_altinn.columns)

indices = [f":{x}" for x in range(1, len(til_altinn.columns) + 1)]
indices = ", ".join(indices)

sql_ins = (
    "INSERT INTO DSBBASE.DLR_ENHET_I_DELREG (" +
    kolonner +
    ") VALUES (" + 
    indices +
    ")"
)
# -

til_altinn["H_VAR1_N"]

# +
import numpy as np

df = til_altinn.copy()

# erstatt NaN/NaT i hele DF med None
df = df.astype(object).where(df.notna(), None)

rows = [tuple(r) for r in df.to_numpy()]

cur = conn.cursor()
if til_lagring and rows:
    try:
        cur.bindarraysize = min(len(rows), 10_000)
        cur.executemany(sql_ins, rows)
        conn.commit()
        print(f"Det er lagt inn {len(rows)} rad(er). Kontroller i SFU.")
    except Exception:
        conn.rollback()
        raise
# -

# ## Slette enheter som skal ut

ut_ident_nr = ut['IDENT_NR'].to_list()

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar2}')
""" 
altinn_raw = pd.read_sql_query(sporring, conn)



ut_identer = altinn_raw[altinn_raw['IDENT_NR'].isin(ut_ident_nr)][['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE']]

ut_identer

# +
import cx_Oracle

# Anta at DataFrame heter ut_identer og inneholder kolonnene 'DELREG_NR', 'IDENT_NR', og 'ENHETS_TYPE'
verdier_til_sletting = ut_identer.values.tolist()
# -
conn.callTimeout = 10_000  # 10 sek (millisekunder)

verdier_til_sletting

# +
# SQL DELETE-kommando tilpasset tabellen
sql_delete = """
DELETE FROM DSBBASE.DLR_ENHET_I_DELREG
WHERE DELREG_NR = :1 AND IDENT_NR = :2 AND ENHETS_TYPE = :3
"""

# Opprett en databaseforbindelse
cur = conn.cursor()

try:
    # Utfør DELETE med executemany for flere rader
    cur.executemany(sql_delete, verdier_til_sletting)

    # Bekreft endringene
    conn.commit()

    print(f"{cur.rowcount} rader slettet.")
except cx_Oracle.DatabaseError as e:
    print(f"En feil oppstod: {e}")
    conn.rollback()  # Rull tilbake hvis noe går galt
# finally:
#     # Lukk cursor og forbindelse
#     cur.close()
#     conn.close()
# -
# # Legge til enheter i nytt delregister 25468
# For å sende brev til private helseforetak med oppdrags- og bestillerdokument

# Definer delregister og enhetstype
ny_delreg_nr = '25468'  # Delregisteret som du vil legge inn i
gammelt_delreg_nr = '2424'  # Delregisteret du henter data fra
enhets_type = 'FRTK'  # Enhetstypen du vil hente
h_var2_a_filter = 'OPPDRAG'  # Filter for H_VAR2_A


# +
# SQL for å sjekke utvalgte enheter
check_sql = """
SELECT enhets_type, orgnr, H_VAR2_A, NAVN
FROM dsbbase.dlr_enhet_i_delreg
WHERE delreg_nr = :gammelt_delreg_nr
  AND enhets_type = :enhets_type
  AND H_VAR2_A = :h_var2_a_filter
"""

# Hent resultatene
try:
    cursor.execute(check_sql, {
        'gammelt_delreg_nr': gammelt_delreg_nr,
        'enhets_type': enhets_type,
        'h_var2_a_filter': h_var2_a_filter
    })
    rows = cursor.fetchall()
    print("Følgende enheter vil bli valgt:")
    for row in rows:
        print(row)
except cx_Oracle.DatabaseError as e:
    error, = e.args
    print(f"Databasefeil: {error.message}")

# -





cursor = conn.cursor()

# SQL-setning for å legge til enheter i TMP-tabellen med filter
sql = """
INSERT INTO dsbbase.dlr_load_enhet_tmp (delreg_nr, enhets_type, orgnr)
SELECT :ny_delreg_nr, enhets_type, orgnr
FROM dsbbase.dlr_enhet_i_delreg
WHERE delreg_nr = :gammelt_delreg_nr 
  AND enhets_type = :enhets_type
  AND H_VAR2_A = :h_var2_a_filter
"""

# Utfør spørringen
try:
    cursor.execute(sql, {
        'ny_delreg_nr': ny_delreg_nr,
        'gammelt_delreg_nr': gammelt_delreg_nr,
        'enhets_type': enhets_type,
        'h_var2_a_filter': h_var2_a_filter  # Legger til den manglende parameteren
    })
    conn.commit()  # Lagre endringene
    print(f"Enheter fra delregister {gammelt_delreg_nr} med enhetstype '{enhets_type}' og H_VAR2_A = '{h_var2_a_filter}' er lagt til i {ny_delreg_nr}.")
except cx_Oracle.DatabaseError as e:
    error, = e.args
    print(f"Databasefeil: {error.message}")
# finally:
    # cursor.close()
    # conn.close()


