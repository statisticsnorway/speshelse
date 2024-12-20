# -*- coding: utf-8 -*-
# +
import pandas as pd
import cx_Oracle
# from db1p import query_db1p
import getpass
import datetime as dt
import requests

til_lagring = False # Sett til True, hvis du skal gjøre endringer i Databasen
# -

import re
import os

import warnings
warnings.filterwarnings('ignore')

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# +
aar4 = 2024
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

idag = dt.date.today().strftime("%Y%m%d")
filsti_output = f"/ssb/stamme01/fylkhels/speshelse/felles/droplister/2023/nytt_delreg/sammenlikne_vof_sfu_{idag}.xlsx"

rapportsamling = {} # variabel som til slutt inneholder alt som skal lagres i Excel-fil



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
# - [ ] Ute etter H_VAR1_A med missing og SEKTOR_2014 ulik "6100". Disse bør listes ut



ut = SFU_enhet[SFU_enhet['ORGNR'].isna()]

rapportsamling['rad i SFU uten ORGNR'] = ut

# Fjerner enheter uten ORGNR
SFU_enhet = SFU_enhet[SFU_enhet['ORGNR'].notnull()]

SFU_enhet.shape

SFU_enhet[['STATUS']].value_counts()

# # Laste inn data

# ##  fra KLASS

# ### Offentlige helseforetak

from klass import get_classification

# +
klass_offentlige_helseforetak = get_classification(603).get_codes()

HF = klass_offentlige_helseforetak.pivot_level()
HF = (
    HF.rename(columns={
        'code_1': 'HELSEREGION',
        'name_2': 'RHF',
        'code_3': 'ORGNR_FORETAK',
        'name_3': 'NAVN_KLASS'
    }
             )
    [['ORGNR_FORETAK', 'NAVN_KLASS', 'HELSEREGION', 'RHF']]
)
# -

RHF = klass_offentlige_helseforetak.pivot_level()

RHF = (
    RHF[['code_1', 'name_2', 'code_2']]
    .drop_duplicates()
    .rename(columns={'code_1': 'HELSEREGION',
                     'name_2': 'NAVN_KLASS',
                     'code_2': 'ORGNR_FORETAK'})
)
RHF['RHF'] = RHF['NAVN_KLASS']

RHF

# ### Private helseinstitusjoner med oppdrags- og bestillerdokument

klass_priv_frtk_ob = get_classification(604).get_codes()

phob = klass_priv_frtk_ob.pivot_level()

phob = (
    phob.rename(columns={
        'code_1': 'HELSEREGION',
        'code_2': 'ORGNR_FORETAK',
        'name_2': 'NAVN_KLASS'
    }
             )
    [['ORGNR_FORETAK', 'NAVN_KLASS', 'HELSEREGION']]
)
phob['RHF'] = None

# ### Regionale og felleseide støtteforetak i spesialisthelsetjenesten

klass_rfss = get_classification(605).get_codes()

rfss = klass_rfss.pivot_level()

rfss = (
    rfss.rename(columns={
        'code_1': 'HELSEREGION',
        'name_2': 'RHF',
        'code_3': 'ORGNR_FORETAK',
        'name_3': 'NAVN_KLASS'
    }
             )
    [['ORGNR_FORETAK', 'NAVN_KLASS', 'HELSEREGION', 'RHF']]
)

rfss2 = (
    get_classification(605)
    .get_codes()
    .data.query("level == '2' & parentCode == '99'")
    [['code', 'parentCode', 'name']]
    .rename(columns={
        'code': 'ORGNR_FORETAK',
        'parentCode': 'HELSEREGION',
        'name': 'NAVN_KLASS'
    }
            )
)
rfss2['RHF'] = "FELLESEIDE STØTTEFORETAK"

rfss3 = (
    get_classification(605)
    .get_codes()
    .data.query("level == '2' & parentCode != '99'")
    [['code', 'parentCode', 'name']]
    .rename(columns={
        'code': 'ORGNR_FORETAK',
        'parentCode': 'HELSEREGION',
        'name': 'NAVN_KLASS'
    }
            )
)
rfss3['RHF'] = None

rapporteringsenheter = pd.concat(
    [HF, RHF, phob, rfss, rfss2, rfss3]
)

# ## Private virksomheter

# Delreg 20877xx er et register med formål å kommunisere med private virksomheter via altinn. Det kan hende at dette ikke er godt nok oppdatert.

# > Hente fra 2423 fra feltet med `"PRIVAT"`. Slippe at 20877xx er oppdatert





sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar_før2}')
"""
SFU_priv = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_priv.shape[0]}\nKolonner: {SFU_priv.shape[1]}")

SFU_priv[SFU_priv['ORGNR'] != SFU_priv['ORGNR_FORETAK']]

SFU_priv[['ORGNR_FORETAK', 'ORGNR', 'NAVN1']]

orgnr_priv_sfu = list(SFU_priv['ORGNR'])

orgnr_priv_ikke_sfu = list(set(orgnr_priv_sfu) - set(SFU_enhet['ORGNR']))

ut = SFU_priv[SFU_priv['ORGNR'].isin(orgnr_priv_ikke_sfu)][['ORGNR', 'NAVN', 'SN07_1', 'STATUS', 'TILSTAND']]

rapportsamling[f'priv. i 20877{aar_før2}, ikke 24{aar2}'] = ut

orgnr_foretak_priv_sfu = list(SFU_priv['ORGNR']) # liste over private virksomheter som ligger i SFU

# ## VOF: liste over alle helseforetak

r_orgnr = rapporteringsenheter.ORGNR_FORETAK.to_numpy()


def lag_sql_str(arr):
    s = "("
    for nr in arr:
        s += "'" + str(nr) + "',"
    s = s[:-1] + ")"
    return s


sql_str_med_private = lag_sql_str(list(r_orgnr) + list(orgnr_foretak_priv_sfu))
sql_str_uten_private = lag_sql_str(list(r_orgnr))

sporring_for = f"""
    SELECT FORETAKS_NR, ORGNR, NAVN, STATUSKODE, TILSTAND, SN07_1, SN07_2, SN07_3
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
        TILSTAND,
        SN07_1,
        SN07_2,
        SN07_3,
        STATUSKODE,
        KARAKTERISTIKK
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
        TILSTAND,
        SN07_1,
        SN07_2,
        SN07_3,
        STATUSKODE,
        KARAKTERISTIKK
    FROM DSBBASE.SSB_BEDRIFT
    WHERE FORETAKS_NR IN {sql_str_foretak_uten_priv}
"""
vof_bdr_uten_private = pd.read_sql_query(sporring_bed_uten_private, conn)

vof_bdr_uten_private.shape

# ## Slå sammen VOF-foretak og -virksomhet
# - Skiller også på med- og uten virksomheter som ligger under private foretak

vof_bdr.sample(1)

vof_for.sample(1)

vof = pd.concat([vof_for, vof_bdr])
vof_uten_private = pd.concat([vof_for_uten_priv, vof_bdr_uten_private])

print(vof.shape)
print(vof_uten_private.shape)

# +
# Ta vare på virksomheter med SN07 start fom. 86 tom 89
# NB: TILSTAND/STATUS == S har ikke SN07_1
# Husk å sjekke SN07_2 og SN07_3
# (
#     vof_bdr
#     .SN07_1
#     .str.split(".")
#     .apply(lambda x: x[0] if x is not None else None)
# )
# -

print("For VOF med alle helseforetak og virksomheter under helseforetak:")
print("Prosent rader med missing i SN07_1:", round(vof.SN07_1.isnull().sum()/vof.shape[0] * 100, 2), "%")
print("Prosent rader med missing i SN07_2:", round(vof.SN07_2.isnull().sum()/vof.shape[0] * 100, 2), "%")
print("Prosent rader med missing i SN07_3:", round(vof.SN07_3.isnull().sum()/vof.shape[0] * 100, 2), "%")

# # Sammenlikne data
# - Filtrere bort STATUS/STATUSKODE != 'B'?

SFU_enhet['NAVN_HEL_SFU'] = (
    SFU_enhet['NAVN1'].fillna("") + " " +
    SFU_enhet['NAVN2'].fillna("") + " " +
    SFU_enhet['NAVN3'].fillna("") + " " +
    SFU_enhet['NAVN4'].fillna("") + " " +
    SFU_enhet['NAVN5'].fillna("") + " " +
    SFU_enhet['SPES_NAVN'].fillna("") + " "
).str.replace(r'\s+', ' ', regex=True)

felles_variabelnavn = [x for x in vof.columns if x in SFU_enhet.columns]

felles_variabelnavn

vof_tilstand = vof[felles_variabelnavn + ['STATUSKODE', 'KARAKTERISTIKK']].copy()
vof_tilstand_uten_private = vof_uten_private[felles_variabelnavn + ['STATUSKODE', 'KARAKTERISTIKK']].copy()
SFU_tilstand = SFU_enhet[felles_variabelnavn + ['STATUS', 'NAVN_HEL_SFU']].copy()

SFU_tilstand.shape

vof_tilstand.shape

vof_tilstand_uten_private.shape

# ## Enhetsanalyse

# ### Sammenlikne virksomheter: SFU -> VOF

# +
orgnr_i_SFU_ikke_VOF = list(set(SFU_tilstand.ORGNR) - set(vof_tilstand.ORGNR))

print(f"Antall virksomheter/foretak som ikke er i delreg 24{aar2}:")
SFU_tilstand[SFU_tilstand.ORGNR.isin(orgnr_i_SFU_ikke_VOF)].shape[0]
# -

ut = SFU_tilstand[SFU_tilstand['ORGNR'].isin(orgnr_i_SFU_ikke_VOF)][['ORGNR', 'NAVN', 'STATUS']]

rapportsamling['i 24, ikke klass eller 20778'] = ut

# ### Private som ikke har missing på HVAR_1_A i SFU:
# Det betyr at det rapporteres tall fra dem på skjema 39?

region = {"03":	"H12",
"11":	"H03",
"15":	"H04",
"18":	"H05",
"31":	"H12",
"32":	"H12",
"33":	"H12",
"34":	"H12",
"39":	"H12",
"40":	"H12",
"42":	"H12",
"46":	"H03",
"50":	"H04",
"55":	"H05",
"56":	"H05",
"99":	"Uoppgitt"}

SFU_enhet['HELSEREGION'] = SFU_enhet['F_KOMMUNENR'].str[:2].map(region)

ut = (
    SFU_enhet[
        (SFU_enhet['H_VAR1_A'].notnull()) &
        (SFU_enhet['H_VAR2_A'] == "PRIVAT")
        # (SFU_enhet['SKJEMA_TYPE'].str.contains("39"))
    ]
    [['ORGNR', 'NAVN_HEL_SFU', 'SN07_1', 'ENHETS_TYPE', 'SKJEMA_TYPE', 'H_VAR1_A', 'H_VAR2_A', 'HELSEREGION']]
)

ut = ut.sort_values(['H_VAR1_A', 'SKJEMA_TYPE'])

rapportsamling['private i SFU'] = ut



# # Sammenlikne STATUS og SN07 i VOF og SFU

# ### Enheter som finnes både i VOF og SFU

sammenlikne_tilstand = pd.merge(
    SFU_enhet,
    vof_tilstand,
    how='inner',
    on='ORGNR',
    suffixes=('_SFU', '_VOF')
)

sammenlikne_tilstand = sammenlikne_tilstand.rename(columns={'STATUSKODE': 'STATUS_VOF', 'STATUS': 'STATUS_SFU'})

kolonner = (
    ['NAVN_HEL_SFU', 'NAVN_VOF'] +
    [x for x in sammenlikne_tilstand.columns if "SN07_" in x] +
    [x for x in sammenlikne_tilstand.columns if "TILSTAND" in x] +
    [x for x in sammenlikne_tilstand.columns if "STATUS" in x]
)

# ### SN07

# Sammenlikner SN07 i begge datasett og filtrerer ut tilfeller der de er ulike. Tar bort tilfeller hvor begge er har missing-verdier.

# +
mask_sn071 = (
    ~((sammenlikne_tilstand['SN07_1_SFU'].isna()) & sammenlikne_tilstand['SN07_1_VOF'].isna()) &
    (sammenlikne_tilstand['SN07_1_SFU'] != sammenlikne_tilstand['SN07_1_VOF'])
)
mask_sn072 = (
    ~((sammenlikne_tilstand['SN07_2_SFU'].isna()) & sammenlikne_tilstand['SN07_2_VOF'].isna()) &
    (sammenlikne_tilstand['SN07_2_SFU'] != sammenlikne_tilstand['SN07_2_VOF'])
)

mask_sn073 = (
    ~((sammenlikne_tilstand['SN07_3_SFU'].isna()) & sammenlikne_tilstand['SN07_3_VOF'].isna()) &
    (sammenlikne_tilstand['SN07_3_SFU'] != sammenlikne_tilstand['SN07_3_VOF'])
)

mask_tilstand = sammenlikne_tilstand['TILSTAND_SFU'] != sammenlikne_tilstand['TILSTAND_VOF']
mask_status = sammenlikne_tilstand['STATUS_SFU'] != sammenlikne_tilstand['STATUS_VOF']

# +
# sammenlikne_tilstand[mask_sn072][['SN07_2_SFU', 'SN07_2_VOF']]
# -

print("Antall med avvik på SN07_1:", sammenlikne_tilstand[mask_sn071][kolonner].shape[0])
print("Antall med avvik på SN07_2:", sammenlikne_tilstand[mask_sn072][kolonner].shape[0])
print("Antall med avvik på SN07_3:", sammenlikne_tilstand[mask_sn073][kolonner].shape[0])

ut = sammenlikne_tilstand[mask_sn071 | mask_sn072 | mask_sn073][kolonner]

rapportsamling['avvik SN07_X'] = ut

# ### Tilstand

print("Antall med avvik på TILSTAND:", sammenlikne_tilstand[mask_tilstand][kolonner].shape[0])

ut = sammenlikne_tilstand[mask_tilstand][['NAVN_HEL_SFU', 'ORGNR', 'TILSTAND_SFU', 'TILSTAND_VOF']]

rapportsamling['avvik TILSTAND'] = ut

# ### Status
#

print("Antall med avvik på STATUS:", sammenlikne_tilstand[mask_status][kolonner].shape[0])

ut = sammenlikne_tilstand[mask_status][['NAVN_HEL_SFU', 'ORGNR', 'STATUS_SFU', 'STATUS_VOF']]

rapportsamling['avvik STATUS'] = ut

# ### Tilstand og status

print("Antall med avvik på STATUS og TILSTAND:", sammenlikne_tilstand[mask_status & mask_tilstand][kolonner].shape[0])

ut = sammenlikne_tilstand[mask_status][['NAVN_HEL_SFU', 'ORGNR', 'STATUS_SFU', 'STATUS_VOF', 'TILSTAND_SFU', 'TILSTAND_VOF']]

rapportsamling['avvik STATUS og TILSTAND'] = ut

# ### Blindpassasjerer

m1 = SFU_enhet['H_VAR1_A'].isna()
m2 = SFU_enhet['SEKTOR_2014'] != '6100'
m3 = SFU_enhet['ORGNR'] != SFU_enhet['ORGNR_FORETAK']
m4 = ~(SFU_enhet['ORGNR_FORETAK'].isin(list(rapporteringsenheter['ORGNR_FORETAK'].unique())))

rapportsamling['Blindpassasjerer'] = SFU_enhet[m1 & m2 & m3 & m4][['NAVN', 'NAVN1', 'NAVN2', 'NAVN3'] + ['ORGNR', 'ORGNR_FORETAK']]

# # Lagring

import sys
import os
sys.path.append(os.path.dirname(os.getcwd()))
from functions.hjelpefunksjoner import lagre_excel



if True:
    lagre_excel(rapportsamling, filsti_output)


