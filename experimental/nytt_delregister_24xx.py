# -*- coding: utf-8 -*-
# +
import pandas as pd
import cx_Oracle
from db1p import query_db1p
import getpass
import datetime as dt
import requests

til_lagring = False # Sett til True, hvis du skal gjøre endringer i Databasen
# -

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# +
aar4 = 2023
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
""" 
SFU_enhet = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_enhet.shape[0]}\nKolonner: {SFU_enhet.shape[1]}")


# Fjerner enheter uten ORGNR
SFU_enhet = SFU_enhet[SFU_enhet['ORGNR'].notnull()]

SFU_enhet[['STATUS']].value_counts()

[x for x in SFU_enhet.columns if "STATUS" in x]

# # Henter data

# ##  fra KLASS

# ### Offentlige helseforetak

# +
URL = f'http://data.ssb.no/api/klass/v1/classifications/603/codes.json?from={aar4}-01-01&includeFuture=True'
r = requests.get(url = URL)
offhelse_df = pd.read_json(r.text)
offhelse_df = pd.json_normalize(offhelse_df['codes'])

RHF_kode_klass = offhelse_df.query('level=="1"')[['code','name']]
RHF_kode_klass = RHF_kode_klass.rename(columns = {'code':'HELSEREGION', 'name': 'RHF'})

lvl2 = offhelse_df.query("level == '2'")
RHF_kode_klass = pd.merge(RHF_kode_klass, lvl2, how="left", left_on="HELSEREGION", right_on="parentCode")
RHF_kode_klass = RHF_kode_klass.rename(columns = {'code': 'ORGNR_FORETAK'})
RHF_kode_klass = RHF_kode_klass[['HELSEREGION', 'RHF', 'ORGNR_FORETAK']]

lvl2 = offhelse_df.query("level == '2'")
lvl3 = offhelse_df.query("level == '3'")
lvl2 = lvl2[['code', 'parentCode', 'name']]
lvl3 = lvl3[['code', 'parentCode', 'name']]

lvl2 = lvl2.rename(columns = {'parentCode':'HELSEREGION', 'name': 'RHF'})
lvl3 = lvl3.rename(columns = {'code':'ORGNR_FORETAK', 'name': 'NAVN_KLASS'})

# +
HF = pd.merge(lvl3, lvl2, how="left", left_on="parentCode", right_on="code")
HF = HF[['ORGNR_FORETAK', 'NAVN_KLASS', 'HELSEREGION', 'RHF']]

RHF_kode_klass['RHF'] = RHF_kode_klass['RHF'] + " RHF"
RHF_kode_klass['NAVN_KLASS'] = RHF_kode_klass['RHF']
# -

# ### Private helseinstitusjoner med oppdrags- og bestillerdokument

# +
URL = f'http://data.ssb.no/api/klass/v1/classifications/604/codes.json?from={aar4}-01-01&includeFuture=True'
r = requests.get(url = URL)
privhelse_df = pd.read_json(r.text)
privhelse_df = pd.json_normalize(privhelse_df['codes'])

temp = privhelse_df.query("level == '2'")
temp = temp[['code', 'parentCode', 'name']]
temp = temp.rename(columns = {'code':'ORGNR_FORETAK','parentCode':'HELSEREGION', 'name': 'NAVN_KLASS'})
temp = pd.merge(temp, RHF_kode_klass, how="left")

rapporteringsenheter = pd.concat([HF,temp])
rapporteringsenheter = pd.concat([rapporteringsenheter,RHF_kode_klass])
# -

# ### Regionale og felleseide støtteforetak i spesialisthelsetjenesten

# +
URL = f'http://data.ssb.no/api/klass/v1/classifications/605/codes.json?from={aar4}-01-01&includeFuture=True'
r = requests.get(url = URL)
regfel_df = pd.read_json(r.text)
regfel_df = pd.json_normalize(regfel_df['codes'])

lvl2o = regfel_df.query("level == '2' & parentCode != '99'")
lvl3o = regfel_df.query("level == '3'")
lvl2o = lvl2o[['code', 'parentCode', 'name']]
lvl3o = lvl3o[['code', 'parentCode', 'name']]
lvl2o = lvl2o.rename(columns = {'parentCode':'HELSEREGION',
                                'name': 'RHF'})
lvl3o = lvl3o.rename(columns = {'code': 'ORGNR_FORETAK',
                                'name': 'NAVN_KLASS'})

temp = pd.merge(lvl3o, lvl2o, how="left", left_on="parentCode", right_on="code")
temp = temp[['ORGNR_FORETAK', 'NAVN_KLASS', 'HELSEREGION', 'RHF']]

# +
rapporteringsenheter = pd.concat([rapporteringsenheter,temp])

temp = regfel_df.query("level == '2' & parentCode == '99'")
temp = temp.rename(columns = {'code':'ORGNR_FORETAK', 'name': 'NAVN_KLASS', 'parentCode': 'HELSEREGION'})
temp['RHF'] = "FELLESEIDE STØTTEFORETAK"
temp = temp[['ORGNR_FORETAK', 'NAVN_KLASS', 'HELSEREGION', 'RHF']]
# -

rapporteringsenheter = pd.concat([rapporteringsenheter,temp])

# ### Limer sammen all data fra KLASS

alle_foretak_virk = pd.concat([offhelse_df, privhelse_df, regfel_df])
alle_foretak_virk = alle_foretak_virk[["code", "parentCode", "level", "name"]]

# ## VOF: liste over alle helseforetak

rapporteringsenheter_uten_RHF = rapporteringsenheter.query('~NAVN_KLASS.str.endswith("RHF")',engine="python")

r_orgnr = rapporteringsenheter_uten_RHF.ORGNR_FORETAK.to_numpy()


def lag_sql_str(arr):
    s = "("
    for nr in arr:
        s += "'" + str(nr) + "',"
    s = s[:-1] + ")"
    return s


# +
sql_str = lag_sql_str(r_orgnr)

sporring_for = f"""
    SELECT FORETAKS_NR, ORGNR, NAVN, STATUSKODE
    FROM DSBBASE.SSB_FORETAK
    WHERE ORGNR IN {sql_str}
"""
vof_for = pd.read_sql_query(sporring_for, conn)

# +
fornummer = pd.Series(vof_for['FORETAKS_NR']).array
sql_str = lag_sql_str(fornummer)

sporring_bed = f"""
    SELECT
        ORGNR,
        NAVN,
        BEDRIFTS_NR_GDATO,
        TILSTAND,
        TILSTAND_RDATO,
        TILSTAND_GDATO,
        TILSTAND_EK,
        STATUSKODE,
        STATUSKODE_RDATO,
        STATUSKODE_GDATO,
        STATUSKODE_EK,
        SN07_1,
        SN07_2,
        SN07_3,
        SN07_1_RDATO,
        SN07_1_GDATO,
        SN07_1_EK,
        SN07_1_EK2,
        SN07_2_RDATO,
        SN07_2_GDATO,
        SN07_2_EK,
        SN07_2_EK2,
        SN07_3_RDATO,
        SN07_3_GDATO,
        SN07_3_EK,
        SN07_3_EK2,
        STATUSKODE
    FROM DSBBASE.SSB_BEDRIFT
    WHERE FORETAKS_NR IN {sql_str}
"""

vof_bdr = pd.read_sql_query(sporring_bed, conn)
# -

vof_bdr

# # Sammenlikne data

import re

SFU_enhet['NAVN_HEL'] = (
    SFU_enhet['NAVN1'].fillna("") + " " +
    SFU_enhet['NAVN2'].fillna("") + " " +
    SFU_enhet['NAVN3'].fillna("") + " " +
    SFU_enhet['NAVN4'].fillna("") + " " +
    SFU_enhet['NAVN5'].fillna("") + " " +
    SFU_enhet['SPES_NAVN'].fillna("") + " "
).str.replace(r'\s+', ' ', regex=True)

SFU_enhet['NAVN_HEL']

SFU_enhet[[x for x in SFU_enhet.columns if "NAVN" in x]].sample(2)

vof_tilstand = vof_bdr[felles_variabelnavn + ['STATUSKODE']].copy()
SFU_tilstand = SFU_enhet[felles_variabelnavn + ['STATUS']].copy()

sammenlikne_tilstand = pd.merge(
    SFU_enhet,
    vof_bdr,
    how='left',
    on='ORGNR',
    suffixes=('_SFU', '_VOF')
)

for x in sammenlikne_tilstand.columns:
    print(x)

kolonner = (
    ['NAVN_HEL', 'NAVN_VOF'] +
    [x for x in sammenlikne_tilstand.columns if "SN07_1" in x] +
    [x for x in sammenlikne_tilstand.columns if "TILSTAND" in x] +
    [x for x in sammenlikne_tilstand.columns if "STATUS" in x]
)

sammenlikne_tilstand[kolonner].sample(1).T




