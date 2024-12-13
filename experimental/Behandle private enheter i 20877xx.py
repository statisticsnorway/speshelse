# # Opprette nytt delregister 20877XX
#
# - Etter kopiering av fjorårets enheter
# - Sjekker kopieringen mot de nye droplistene
# - Legger inn nye og fjerner utgåtte enheter

# +
import pandas as pd
import cx_Oracle
from db1p import query_db1p
import getpass

from datetime import datetime
import datetime as dt
til_lagring = False # Sett til True, hvis du skal gjøre endringer i Databasen
# -

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

sti += filnavn

sti

pop = pd.read_csv(sti, encoding='latin1', sep=";", dtype="object")

len(pop)

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

nye = sette_sammen[(sette_sammen['_merge'] == 'right_only') & (sette_sammen['SKJEMA_TYPE'] == '381 441 451 461 47')]

nye



ut = sette_sammen.query('_merge == "left_only"').copy()

ut





sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU = pd.read_sql_query(sporring, conn)
print(f"Rader:    {altinn_raw.shape[0]}\nKolonner: {altinn_raw.shape[1]}")

skjematyper_per_foretak = SFU[SFU['H_VAR2_A'] == 'PRIVAT'].groupby(['ORGNR_FORETAK']).SKJEMA_TYPE.unique()

# +
# skjematyper_per_foretak
# -



# +
## Lage skjema navn utfra SKJEMA_TYPE-variabel:
# nye['SKJEMA'] = nye['SKJEMA_TYPE'].str.split(" ").apply(lambda x: ", HELSE".join(x))
# nye['SKJEMA'] = nye['SKJEMA'].str.replace("1", "P")
# nye['SKJEMA'] = nye['SKJEMA'].apply(lambda x: "HELSE" + x)
# nye['FORETAK'] = "(" + nye['ORGNR_FORETAK'] + ") " + nye['FORETAK_NAVN']
# nye
# -



# ## Hente inn enheter i siste 20877XX

# ## Bestemme ny populasjon

ny_altinn

# DELREGISTER, IDENTNUMMER, ENHETSTYPE
#
#
# ORGNUMMER er ikke viktig for kobling av tabeller. IDENTNUMMER kommer fra VoF. 



# ### Endringer
# Dersom det skal gjøres endringer i 20877xx, listes disse opp her:

print(sammen[sammen['status'].isin(['inn', 'ut'])][['ORGNR_FORETAK', 'FORETAK_NAVN', 'NAVN', 'status']].to_markdown(index=False))

print("INN:")
display(sammen[sammen['status'].isin(['inn'])]['ORGNR_FORETAK'].to_list())
print("UT:")
display(sammen[sammen['status'].isin(['ut'])]['ORGNR_FORETAK'].to_list())





# # Eksperimentelt

# ## Gjøre endringer i delreg 20877XX

# Henter inn informasjon om de nye enhetene fra 2423:

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
""" 
SFU_enhet = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_enhet.shape[0]}\nKolonner: {SFU_enhet.shape[1]}")

orgnr_inn = list(sammen[sammen['status'] == 'inn']['ORGNR_FORETAK'].unique())

len(orgnr_inn)

sammen[sammen['status'] == 'inn']

til_altinn = SFU_enhet[(SFU_enhet['ORGNR_FORETAK'].isin(orgnr_inn)) & SFU_enhet['H_VAR2_N'].notnull()].copy()

# Omorganiser kun de felles kolonnene, og behold de unike kolonnene som de er
felles_kol = [col for col in altinn_raw.columns if col in til_altinn.columns]
til_altinn = til_altinn[felles_kol + [col for col in til_altinn.columns if col not in felles_kol]]

altinn_raw.columns

til_altinn.columns

# Dobbeltsjekk at enhetene ikke allerede ligger i delregisteret
assert len(altinn_raw[altinn_raw.ORGNR_FORETAK.isin(orgnr_inn)]) == 0, "Enheter ligger allerede i systemet"


# +
# til_altinn[[col for col in til_altinn.columns if "DATO" in col]]
# -

def tile_df(df, num_cols, num_rows):
    n = len(df.columns)
    num_rows = min(num_rows, len(df))

    for i in range(0, n, num_cols):
        if i + num_cols < n:
            display(df.iloc[:, i:i + num_cols].sample(num_rows))
        else:
            display(df.iloc[:, i:].sample(num_rows))


# ## Tilpasse data som skal inn i delregisteret

til_altinn['DELREG_NR'] = f"20877{aar2}"
til_altinn['DATO_INSERT'] = pd.Timestamp.now().floor('S')
til_altinn['USER_INSERT'] = getpass.getuser().upper()
til_altinn['DATO_UPDATE'] = None
til_altinn['USER_UPDATE'] = None

til_altinn[["DELREG_NR", "DATO_INSERT", "USER_INSERT", "DATO_UPDATE", "USER_UPDATE"]].sample(2)

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
# kolonner = ", ".join(til_altinn.columns)

# indices = [f":{x}" for x in range(1, len(til_altinn.columns) + 1)]
# indices = ", ".join(indices)

# sql_ins = (
#     "INSERT INTO DSBBASE.DLR_ENHET_I_DELREG (" +
#     kolonner +
#     ") VALUES (" + 
#     indices +
#     ")"
# )

# +
# # Oppretter skrivekontakt med Oracle
# cur = conn.cursor()

# # Stabler om dataframen til SQL-vennlig innlesing
# rows = [tuple(x) for x in foretak_til_innkvittering_df.values]

# # Hvis til_lagring = True kjøres SQL-inserten
# if til_lagring and len(rows) != 0:
#     cur.executemany(sql_ins, rows)
#     conn.commit()
#     print(f"Det er gjort {len(rows)} radendringer. Kontroller i SFU.")

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

# +
import cx_Oracle

# Anta at DataFrame heter df og inneholder kolonnene 'DELREG_NR', 'IDENT_NR', og 'ENHETS_TYPE'
verdier_til_sletting = ut_identer.values.tolist()

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
finally:
    # Lukk cursor og forbindelse
    cur.close()
    conn.close()
# -



ut











# +
# # Definerer SQL DELETE-kommandoen
# # Anta at vi vil slette rader basert på en spesifikk identifikator
# sql_delete = "DELETE FROM din_tabell WHERE IDENT_NR = :1"

# # Verdi som identifiserer radene som skal slettes
# ident_nr_til_sletting = 'ID1'

# # Oppretter en cursor fra din databaseforbindelse
# cur = conn.cursor()

# # Utfører DELETE-kommandoen
# cur.execute(sql_delete, [ident_nr_til_sletting])

# # Commiter endringene til databasen
# conn.commit()

# print("Rader slettet.")
# -













orgnr_ut = list(sammen[sammen['status'] == "ut"]['ORGNR_FORETAK'])

orgnr_ut



















sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('138555{aar2}')
"""
idun = pd.read_sql_query(sporring, conn)


