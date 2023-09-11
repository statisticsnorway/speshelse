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

import warnings
warnings.filterwarnings('ignore')

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


# - [ ] Sjekk virksomhet med NULL ORGNR
# - [x] Hva står B, S og F for i STATUS-variabelen
#     - B: ikke slettet
#     - S: slettet
#     - D: slettet som dublett
#     - F: slette for sammenslåing

# Fjerner enheter uten ORGNR
SFU_enhet = SFU_enhet[SFU_enhet['ORGNR'].notnull()]

SFU_enhet.shape

SFU_enhet[['STATUS']].value_counts()

SFU_enhet.sample(1)

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

# # Private virksomheter

# Delreg 20877xx er et register med formål å kommunisere med private virksomheter via altinn. Det kan hende at dette ikke er godt nok oppdatert.

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar_før2}')
"""
SFU_priv = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_priv.shape[0]}\nKolonner: {SFU_priv.shape[1]}")

orgnr_priv_sfu = list(SFU_priv['ORGNR'])

orgnr_priv_ikke_sfu = list(set(orgnr_priv_sfu) - set(SFU_enhet['ORGNR']))

SFU_priv = SFU_priv[~SFU_priv['ORGNR'].isin(orgnr_priv_ikke_sfu)]

orgnr_foretak_priv_sfu = list(SFU_priv['ORGNR']) # liste over private virksomheter som ligger i SFU

# ## VOF: liste over alle helseforetak

rapporteringsenheter_uten_RHF = rapporteringsenheter.query('~NAVN_KLASS.str.endswith("RHF")',engine="python")

r_orgnr = rapporteringsenheter_uten_RHF.ORGNR_FORETAK.to_numpy()


def lag_sql_str(arr):
    s = "("
    for nr in arr:
        s += "'" + str(nr) + "',"
    s = s[:-1] + ")"
    return s


sql_str_med_private = lag_sql_str(list(r_orgnr) + list(orgnr_foretak_priv_sfu))
sql_str_uten_private = lag_sql_str(list(r_orgnr))

sporring_for = f"""
    SELECT FORETAKS_NR, ORGNR, NAVN, STATUSKODE
    FROM DSBBASE.SSB_FORETAK
    WHERE ORGNR IN {sql_str_med_private}
"""
vof_for = pd.read_sql_query(sporring_for, conn)

sporring_for_uten_priv = f"""
    SELECT FORETAKS_NR, ORGNR, NAVN, STATUSKODE
    FROM DSBBASE.SSB_FORETAK
    WHERE ORGNR IN {sql_str_uten_private}
"""
vof_for_uten_priv = pd.read_sql_query(sporring_for_uten_priv, conn)

print("Antall foretak som ikke har B som STATUSKODE:", vof_for[vof_for['STATUSKODE'] != 'B'].shape[0])
display(vof_for[vof_for['STATUSKODE'] != 'B'].NAVN)

fornummer = pd.Series(vof_for['FORETAKS_NR']).array
sql_str = lag_sql_str(fornummer)

fornummer_uten_private = pd.Series(vof_for_uten_priv['FORETAKS_NR']).array
sql_str_foretak_uten_priv = lag_sql_str(fornummer_uten_private)

sporring_bed = f"""
    SELECT
        ORGNR,
        FORETAKS_NR,
        NAVN,
        BEDRIFTS_NR_GDATO,
        TILSTAND,
        STATUSKODE,
        SN07_1,
        SN07_2,
        SN07_3,
        STATUSKODE
    FROM DSBBASE.SSB_BEDRIFT
    WHERE FORETAKS_NR IN {sql_str}
"""
vof_bdr = pd.read_sql_query(sporring_bed, conn)

vof_bdr.shape

sporring_bed_uten_private = f"""
    SELECT
        ORGNR,
        FORETAKS_NR,
        NAVN,
        BEDRIFTS_NR_GDATO,
        TILSTAND,
        STATUSKODE,
        SN07_1,
        SN07_2,
        SN07_3,
        STATUSKODE
    FROM DSBBASE.SSB_BEDRIFT
    WHERE FORETAKS_NR IN {sql_str_foretak_uten_priv}
"""
vof_bdr_uten_private = pd.read_sql_query(sporring_bed_uten_private, conn)





# Ta vare på virksomheter med SN07 start fom. 86 tom 89
# NB: TILSTAND/STATUS == S har ikke SN07_1
# Husk å sjekke SN07_2 og SN07_3
vof_bdr.SN07_1.str.split(".").apply(lambda x: x[0] if x is not None else None)

print("Antall prosent rader uten missing i SN07_1:", round(vof_bdr.SN07_1.notnull().sum()/vof_bdr.shape[0] * 100,2), "%")
print("Antall prosent rader uten missing i SN07_2:", round(vof_bdr.SN07_2.notnull().sum()/vof_bdr.shape[0] * 100,2), "%")
print("Antall prosent rader uten missing i SN07_3:", round(vof_bdr.SN07_3.notnull().sum()/vof_bdr.shape[0] * 100,2), "%")

# # Sammenlikne data
# - Filtrere bort STATUS/STATUSKODE != 'B'

import re

SFU_enhet['NAVN_HEL'] = (
    SFU_enhet['NAVN1'].fillna("") + " " +
    SFU_enhet['NAVN2'].fillna("") + " " +
    SFU_enhet['NAVN3'].fillna("") + " " +
    SFU_enhet['NAVN4'].fillna("") + " " +
    SFU_enhet['NAVN5'].fillna("") + " " +
    SFU_enhet['SPES_NAVN'].fillna("") + " "
).str.replace(r'\s+', ' ', regex=True)

# +
# Tar ut foretak i SFU
mask = SFU_enhet['ORGNR_FORETAK'] == SFU_enhet['ORGNR']

SFU_virk = SFU_enhet[~mask].copy()
# -

SFU_virk.shape

felles_variabelnavn = [x for x in vof_bdr.columns if x in SFU_virk.columns]

felles_variabelnavn

vof_tilstand = vof_bdr[felles_variabelnavn + ['STATUSKODE']].copy()
vof_tilstand_uten_private = vof_bdr_uten_private[felles_variabelnavn + ['STATUSKODE']].copy()
SFU_tilstand = SFU_virk[felles_variabelnavn + ['STATUS', 'NAVN_HEL']].copy()

SFU_tilstand.shape

vof_tilstand.shape

# ## Sammenlikne virksomheter: SFU -> VOF

orgnr_i_SFU_ikke_VOF = list(set(SFU_tilstand.ORGNR) - set(vof_tilstand.ORGNR))

print("Antall virksomheter som ikke har registrerte foretak i delreg 2423:")
SFU_tilstand[SFU_tilstand.ORGNR.isin(orgnr_i_SFU_ikke_VOF)].shape[0]

orgnr_i_VOF_ikke_SFU = list(set(vof_tilstand.ORGNR) - set(SFU_tilstand.ORGNR))

# ## Sammenlikne virksomheter: VOF -> SFU

print("Antall virksomheter som ligger under alle offentlige og private helseforetak,")
print("men som ikke har match i SFU:")
vof_tilstand[vof_tilstand.ORGNR.isin(orgnr_i_VOF_ikke_SFU)].shape[0]

vof_tilstand[vof_tilstand.ORGNR.isin(orgnr_i_VOF_ikke_SFU)].sample(5)

# ## Sammenlikne virksomheter: SFU -> VOF (uten private)

orgnr_i_VOF_ikke_SFU_uten_private = list(set(vof_tilstand_uten_private.ORGNR) - set(SFU_tilstand.ORGNR))

print("Antall virksomheter som ligger under alle offentlige og private helseforetak,")
print("men som ikke har match i SFU:")
vof_tilstand_uten_private[vof_tilstand_uten_private.ORGNR.isin(orgnr_i_VOF_ikke_SFU_uten_private)].shape[0]

vof_tilstand_uten_private[vof_tilstand_uten_private['ORGNR'].isin(orgnr_i_VOF_ikke_SFU_uten_private)].sample(5)

# ### Funn

# - 97 virksomheter som ikke har registrerte foretak i delreg 2423
# - 309 virksomheter i VOF som ikke er i delreg 2423











# # Noe annet (se på senere)

sammenlikne_tilstand = pd.merge(
    SFU_enhet,
    vof_bdr,
    how='left',
    on='ORGNR',
    suffixes=('_SFU', '_VOF')
)

kolonner = (
    ['NAVN_HEL', 'NAVN_VOF'] +
    [x for x in sammenlikne_tilstand.columns if "SN07_1" in x] +
    [x for x in sammenlikne_tilstand.columns if "TILSTAND" in x] +
    [x for x in sammenlikne_tilstand.columns if "STATUS" in x]
)

sammenlikne_tilstand[kolonner].sample(1).T




