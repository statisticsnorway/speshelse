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

SFU_enhet.ORGNR_FORETAK.duplicated().sum()

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
common_columns = [col for col in altinn_raw.columns if col in til_altinn.columns]
til_altinn = til_altinn[common_columns + [col for col in til_altinn.columns if col not in common_columns]]

altinn_raw.columns

til_altinn.columns

# Dobbeltsjekk at enhetene ikke allerede ligger i delregisteret
assert len(altinn_raw[altinn_raw.ORGNR_FORETAK.isin(orgnr_inn)]) == 0

display(til_altinn.sample(1))
display(altinn_raw.sample(1))

rows = [tuple(x) for x in til_altinn.values]

# ## PASSE PÅ
# ENDRE `DATO_MED_I_DELREG`, `DATO_INSERT`, `DATO_UPDATE`, `USER_UPDATE` ? 








