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
til_lagring = True # Sett til True, hvis du skal gjøre endringer i Databasen
# -

import os

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# +
aar4 = 2023
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

# ## Importer siste brukerliste

dropliste_sti = f"/ssb/stamme01/fylkhels/speshelse/felles/droplister/{aar4}/"

os.path.exists(dropliste_sti)

droplistemapper = os.listdir(dropliste_sti)

print(droplistemapper)

date_folders = [f for f in droplistemapper if f.isdigit() and len(f) == 6]
date_folders.sort(key=lambda x: datetime.strptime(x, '%d%m%y'))
siste_dropliste_dato = date_folders[-1]

print(date_folders)

print(siste_dropliste_dato)

sti = dropliste_sti + siste_dropliste_dato + "/"
os.listdir(sti)

filnavn = "Brukerliste_2023_041223.csv"
sti = sti + filnavn

pop = pd.read_csv(sti, encoding='latin1', sep=";", dtype="object")

pop.sample(3)

priv_pop = pop.loc[pop.SKJEMA_TYPE.str.contains("39")]

display(priv_pop.sample(3))
print(f"Rader:    {priv_pop.shape[0]}\nKolonner: {priv_pop.shape[1]}")

# ## Hente inn enheter i siste 20877XX

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar2}')
"""
altinn_raw = pd.read_sql_query(sporring, conn)
print(f"Rader:    {altinn_raw.shape[0]}\nKolonner: {altinn_raw.shape[1]}")

altinn = altinn_raw[['IDENT_NR', 'ORGNR', 'ORGNR_FORETAK', 'NAVN']]

altinn.sample(3)

# ## Sammenlikne altinn og ny populasjon

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

# ## Bestemme ny populasjon

sammen.loc[sammen['ORGNR_FORETAK'] == "982791952", "status"] = "behold"

m1 = sammen["status"] == "behold"
m2 = sammen["status"] == "inn"
ny_altinn = sammen[m1 | m2].copy()

len(ny_altinn)
ny_altinn.sample(3)

m1 = sammen["status"] == "ut"

tas_ut = sammen[m1]

tas_ut

print(sammen[sammen['status'].isin(['inn', 'ut'])][['ORGNR_FORETAK', 'FORETAK_NAVN', 'NAVN', 'status']].to_markdown(index=False))



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

orgnr_ut = list(sammen[sammen['status'] == "ut"]['ORGNR_FORETAK'])

orgnr_ut








