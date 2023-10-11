# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: spesh
#     language: python
#     name: spesh
# ---

# + [markdown] toc-hr-collapsed=true
# # Introduksjon
# -

# ## Forfatter(e)
# 1. Magne Furuholmen Myhren . Basert på en rekke sas-filer, tidligere drop-lister og spesifikk kompetanse fra kollegaer (30.09.22).

# ## Forklaring
#
# Denne notebooken bruker data fra SFU, KLASS og DYNAREV til å sette sammen _droplister_ til hvert skjema spesialisthelsetjenesten sender ut. Droplister er .csv-filer som inneholder en oversikt over alle foretak som har rapporteringsansvar, enten bare for seg selv eller for undervirksomheter. 
#
# For at filene skal passe med KOSTRA-IT sine programmer, må de ha en bestemt utforming hvor kolonnenes rekkefølge har betydning og noen kolonner har særskilte krav når det kommer linjeskift og tegnsetting.
#
# Notebooken er en fullverdig erstatning for et helt løp tidligere gjort i SAS.

# ## Følgende datakilder må være oppdatert
#
# - [x] Standardtabell 603, 604 og 605 i KLASS
# - [x] SFU (DYNAREV delregister 24xx og 19377xx)

# ## Oversikt over skjema
# ```Variabel``` er variabelnavnet som peker til droplisten.

#   Variabel      |  Skjemanavn      |  Forklaring                                       
# ----------------|------------------|-------------------------------------------------------
#   skj0X         |  0X              |  Resultatregnskap for RFH og HF                       
#   skj0Y         |  0Y              |  Balanseregnskap for RFH og HF                        
#   skj38O        |  HELSE38O        |  TSB for offentlige helseforetak                      
#   skj38P        |  HELSE38P        |  TSB for private foretak                              
#   skj39         |  HELSE39         |  Resultatregnskap for private                         
#   skj40         |  HELSE40         |  Kontantstrømoppstilling                              
#   skj41         |  HELSE41         |  Private spesialister med driftsavtale                
#   skj44O        |  HELSE44O        |  Psykisk helsevern for voksne, offentlige             
#   skj44P        |  HELSE44P        |  Psykisk helsevern for voksne, private                
#   skj45O        |  HELSE45O        |  Psykisk helsevern for barn og unge, offentlige       
#   skj45P        |  HELSE45P        |  Psykisk helsevern for barn og unge, private          
#   skj46O        |  46O             |  Somatiske sykehus, offentlige                        
#   skj46P        |  HELSE46P        |  Somatiske sykehus, private                           
#   skj47         |  HELSE47         |  Somatiske institusjoner                              
#   skj48         |  HELSE48         |  Praksiskonsulentordningen                            

# + [markdown] toc-hr-collapsed=true
# # Setup
# -

# ## Importerer pakker og setter innstillinger

import numpy as np
import pandas as pd
import cx_Oracle
# from db1p import query_db1p
import getpass
import os
from klass import get_classification
import requests
from pandas.io.json import json_normalize #package for flattening json in pandas df

# +
# Fjerner begrensning på antall rader og kolonner som vises av gangen
pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

# Unngå standardform i output
pd.set_option('display.float_format', lambda x: '%.0f' % x)
# -

# ## Velger årgang og delregister

# NB! For å lagre filene til angitt sti må `lagre_filer` settes til `True`.

# +
aargang = 2023
siste_to_siffer_aargang = str(aargang)[-2:]

aar_for = aargang - 1            # året før
siste_to_siffer_aar_for = str(aar_for)[-2:]

dato_idag = pd.Timestamp("today").strftime("%d%m%y")

###############################################################################
lagre_filer = True              # sett til True, hvis du vil lagre droplistene
###############################################################################
stamme_sti = "/ssb/stamme01/fylkhels/speshelse/felles/droplister/"
# -

# lagrer i mappe sortert på aargang og dagens dato. Lager ny mappe hvis
# den allerede ikke eksisterer
if lagre_filer:
    if not os.path.exists(stamme_sti + str(aargang)):
        os.makedirs(stamme_sti + str(aargang))

    if not os.path.exists(stamme_sti + str(aargang) + "/" + dato_idag):
        os.makedirs(stamme_sti + str(aargang) + "/" + dato_idag)

    sti_til_lagring = stamme_sti + str(aargang) + "/" + dato_idag + "/"

# ## Tilgang oracle

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# # Henter data

# + [markdown] toc-hr-collapsed=true
# ##  fra KLASS
# -

# ### Offentlige helseforetak

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

RHF_kode_klass = klass_offentlige_helseforetak.pivot_level()
RHF_kode_klass = (
    RHF_kode_klass[['code_1', 'name_2', 'code_2']]
    .drop_duplicates()
    .rename(columns={'code_1': 'HELSEREGION',
                     'name_2': 'NAVN_KLASS',
                     'code_2': 'ORGNR_FORETAK'})
)
RHF_kode_klass['RHF'] = RHF_kode_klass['NAVN_KLASS']

# + [markdown] toc-hr-collapsed=true
# ### Private helseinstitusjoner med oppdrags- og bestillerdokument

# +
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
# -

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

rfss3

rfss_region = klass_rfss.data.copy()

rfss_region = rfss_region[rfss_region['level'] == '1'][['code', 'name']].rename(columns={'code': 'HELSEREGION', 'name': 'RHF'})

rapporteringsenheter = pd.concat(
    [HF, RHF, RHF_kode_klass, rfss, rfss2, rfss3]
)

rapporteringsenheter = rapporteringsenheter.drop(columns=['RHF'])

rapporteringsenheter = pd.merge(rapporteringsenheter,
                                rfss_region,
                                how='left',
                                on='HELSEREGION'
                                )

# ### Limer sammen all data fra KLASS

alle_foretak_virk = pd.concat([offhelse_df, privhelse_df, regfel_df])
alle_foretak_virk = alle_foretak_virk[["code", "parentCode", "level", "name"]]

# ## VOF: liste over alle helseforetak

rapporteringsenheter_uten_RHF = rapporteringsenheter.query('~NAVN_KLASS.str.endswith("RHF")',engine="python")

r_orgnr = rapporteringsenheter_uten_RHF.ORGNR_FORETAK.to_numpy()


# +
def lag_sql_str(arr):
    s = "("
    for nr in arr:
        s += "'" + str(nr) + "',"
    s = s[:-1] + ")"
    return s

sql_str = lag_sql_str(r_orgnr)

sporring_for = f"""
    SELECT FORETAKS_NR, ORGNR, NAVN
    FROM DSBBASE.SSB_FORETAK
    WHERE STATUSKODE = 'B' AND ORGNR IN {sql_str}
"""
vof_for = pd.read_sql_query(sporring_for, conn)

fornummer = pd.Series(vof_for['FORETAKS_NR']).array
sql_str = lag_sql_str(fornummer)

sporring_bed = f"""
    SELECT FORETAKS_NR, ORGNR, NAVN, KARAKTERISTIKK, SN07_1, SB_TYPE
    FROM DSBBASE.SSB_BEDRIFT
    WHERE STATUSKODE = 'B' AND FORETAKS_NR IN {sql_str}
"""
vof_bdr = pd.read_sql_query(sporring_bed, conn)

# -

# Henter organisasjons- og foretaksnummer fra Virksomhets- og foretaksregisteret (VoF) og samler disse i én tabell kalt ```vof```

# +
vof_for = vof_for.rename(columns={'NAVN': 'NAVN_FORETAK'})
vof_for = vof_for.rename(columns={'ORGNR': 'ORGNR_FORETAK'})

vof_bdr = vof_bdr.rename(columns = {'ORGNR':'ORGNR_BEDRIFT'})
vof_bdr['KARAKTERISTIKK'] = vof_bdr['KARAKTERISTIKK'].fillna("")
vof_bdr['NAVN_BEDRIFT'] = vof_bdr['NAVN'] + " " + vof_bdr['KARAKTERISTIKK']

vof_bdr=vof_bdr.drop(columns=['NAVN', 'KARAKTERISTIKK'])

vof = pd.merge(vof_bdr,vof_for, how='left', on='FORETAKS_NR')
vof = vof.drop(columns=['FORETAKS_NR'])

rapporteringsenheter['ORGNR_FORETAK'] = rapporteringsenheter['ORGNR_FORETAK'].apply(str)
rapporteringsenheter_vof = pd.merge(vof,rapporteringsenheter, how='left', on='ORGNR_FORETAK')
# -

rapporteringsenheter_vof.info()

# ## Dynarev

# Henter ut SFU og tar senere vare på de som har har non-missing på ```SKJEMA_TYPE```.

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{siste_to_siffer_aargang}')
"""
SFU_data = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_data.shape[0]}\nKolonner: {SFU_data.shape[1]}")
SFU_data.info()


# Enheter med verdi i 'KVITT_TYPE' filtreres ut (de er nedlagte enheter). Fjerner også enheter uten ORGNR
SFU_data = SFU_data[~SFU_data['KVITT_TYPE'].notnull()]
SFU_data = SFU_data[SFU_data['ORGNR'].notnull()]


def hent_data_delreg24x_og_19377x(x, skjema):
    sporring = f"""
        SELECT *
        FROM DYNAREV.VW_SKJEMA_DATA
        WHERE DELREG_NR IN ('24{x}', '19377{x}') AND SKJEMA IN ('{skjema}') AND AKTIV = '1'
    """
    sporring_df = pd.read_sql_query(sporring, conn)
    sporring_df = sporring_df[['SKJEMA','ENHETS_ID', 'FELT_ID', 'FELT_VERDI']]
    sporring_df = sporring_df.pivot(index=['ENHETS_ID','SKJEMA'], columns='FELT_ID', values='FELT_VERDI')
    return sporring_df


skjemadata38O = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE38O')
skjemadata38P = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE38P')
skjemadata39  = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE39')
skjemadata40  = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE40')
skjemadata41  = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE41')
skjemadata44O = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE44O')
skjemadata44P = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE44P')
skjemadata45O = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE45O')
skjemadata45P = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE45P')
skjemadata46O = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, '46O')           # skj46 bytter navn for ulike aarganger (fiks med klass?)
skjemadata46P = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE46P')
skjemadata47  = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE47')
skjemadata48  = hent_data_delreg24x_og_19377x(siste_to_siffer_aar_for, 'HELSE48')

# # Setter sammen master-SFU-fil med all nødvendig informasjon

# SFU: orgnummer, orgnummer_foretak, foretaksnavn, helseregionnavn, helseregionsnummer
# "alle virksomhetene til speshelse ligger i 2421"
#

# +
SFUklass = pd.merge(SFU_data, rapporteringsenheter, how="left", on="ORGNR_FORETAK")

SFUklass['HELSEREGION'] = SFUklass['HELSEREGION'].fillna("06")

SFUklass['SKJEMA_TYPE'] = SFUklass['SKJEMA_TYPE'].apply(lambda x: str(x).split(" "))
SFUklass[['NAVN1', 'NAVN2', 'NAVN3', 'NAVN4', 'NAVN5']] = SFUklass[['NAVN1', 'NAVN2', 'NAVN3', 'NAVN4', 'NAVN5']].fillna("")
# -

# Slår sammen alle navnekolonnene til en adskilt med ett mellomrom
SFUklass['NAVN'] = SFUklass['NAVN1'] + " " +\
                   SFUklass['NAVN2'] + " " +\
                   SFUklass['NAVN3'] + " " +\
                   SFUklass['NAVN4'] + " " +\
                   SFUklass['NAVN5']
SFUklass['NAVN'] = SFUklass['NAVN'].apply(lambda x: ' '.join(x.split()))

# # Lager droplister
# Sortert etter hva slags dropliste som skal lages.

# En liste over alle variabelnavn som peker til droplister
skjemaer_til_droplister = ["skj0X", "skj0Y", "skj38O", "skj38P", "skj39", "skj40", "skj41",
                           "skj44O", "skj44P", "skj45O", "skj45P", "skj46O",
                           "skj46P", "skj47", "skj48"]


# Funksjon som returnerer en tabell med alle rader som inneholder et gitt skjema:

# eks:
# tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "0X")
def tabell_som_inneholder_skjema(df, kol, skjemanavn):
    ant = df.shape[0]
    rader_med_skjemanavn = []
    for i in range(ant):
        if skjemanavn in df[kol].iloc[i]:
            rader_med_skjemanavn.append(i)
    return df.iloc[rader_med_skjemanavn]


# + [markdown] toc-hr-collapsed=true
# ## Skjemaer som hentes rett fra klass og SFU
# -

# ### 0X (Resultatregnskap for RHF og HF)

# +
# kolonner som skal være med i droplisten:
kolonner0X0Y404148 = ["USERID", "REGION_NR", "REGION_NAVN", "FORETAK_ORGNR", "FORETAK_NAVN"]

skj0X = tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "0X")
skj0X = skj0X.rename(columns = {'NAVN1':'FORETAK_NAVN',
                                'ORGNR':'USERID',
                                'HELSEREGION':'REGION_NR',
                                'RHF':'REGION_NAVN'})
skj0X['FORETAK_ORGNR'] = skj0X['USERID']

skj0X = skj0X[kolonner0X0Y404148].sort_values('REGION_NR')
# -

# ### 0Y (Balanseregnskap for RHF og HF)

# +
skj0Y = tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "0Y")
skj0Y = skj0Y.rename(columns = {'NAVN1':'FORETAK_NAVN', 
                                'ORGNR':'USERID', 
                                'HELSEREGION':'REGION_NR', 
                                'RHF':'REGION_NAVN'})
skj0Y['FORETAK_ORGNR'] = skj0Y['USERID']

skj0Y = skj0Y[kolonner0X0Y404148].sort_values('REGION_NR')
# -

# ### skj40 (Kontantstrømoppstilling)

# +
skj40 = tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "0Y")
skj40 = skj40.rename(columns = {'NAVN1':'FORETAK_NAVN',
                                'ORGNR':'USERID',
                                'HELSEREGION':'REGION_NR',
                                'RHF':'REGION_NAVN'})
skj40['FORETAK_ORGNR'] = skj40['USERID']

skj40 = skj40[kolonner0X0Y404148].sort_values('REGION_NR')
# -

# ### skj41 (Private spesialister med driftsavtale)

skj41 = RHF_kode_klass.copy()
skj41 = skj41.rename(columns = {'HELSEREGION': 'REGION_NR',
                                'RHF': 'REGION_NAVN',
                                'ORGNR_FORETAK': 'FORETAK_ORGNR'})
skj41['USERID'] = skj41['FORETAK_ORGNR']
skj41['FORETAK_NAVN'] = skj41['REGION_NAVN']
skj41 = skj41[kolonner0X0Y404148]

# ### skj48 (Praksiskonsulentordningen)

# +
skj48 = tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "48").copy()
skj48 = skj48[["NAVN_KLASS", "ORGNR_FORETAK", "HELSEREGION"]]
skj48 = pd.merge(skj48, RHF_kode_klass[['HELSEREGION', 'RHF']], how="left", on="HELSEREGION")
skj48 = pd.concat([skj48, HF])
skj48 = skj48.rename(columns={"NAVN_KLASS": "FORETAK_NAVN",
                              "ORGNR_FORETAK": "FORETAK_ORGNR",
                              "HELSEREGION": "REGION_NR",
                              "RHF": "REGION_NAVN"})
skj48['USERID'] = skj48['FORETAK_ORGNR']

skj48 = skj48[kolonner0X0Y404148] \
    .sort_values("REGION_NR") \
    .reset_index(drop=True)


# + [markdown] toc-hr-collapsed=true
# ## Skjemaer til offentlige foretak
# Inneholder et visst antall kolonner med virksomheters navn og orgnr.
# -

def lag_navn_orgnr_kolonnenavn(ant_kolonner):
    nvn, org = [], []
    for i in range(1,ant_kolonner+1):
        nvn.append(f'NAVN_VIRK{i}')
        org.append(f'ORGNR_VIRK{i}')
    return nvn, org


# Denne funksjonen tar inn en dataframe med undervirksomheter som hører til et foretaksnummer og antall kolonner som skal være med i droplisten. Undervirksomhetene legges etterhverandre med riktig korrespondanse mellom navn- og orgnr-kolonner.

def lag_navn_orgnr_kolonner(fvdf, ant_kolonner, med_foretak = True):
    foretak_rader = fvdf.ORGNR_FORETAK.value_counts().index.to_numpy()              # tar ut unike foretaksnummer i en liste

    foretak_org_df = -1
    for nr in foretak_rader:                                                        # blar gjennom listen og lager kolonner til hvert foretak
        if med_foretak:
            orgnr_df = fvdf.query(f'ORGNR_FORETAK  == "{nr}"')['ORGNR']
            navn_df = fvdf.query(f'ORGNR_FORETAK  == "{nr}"')['NAVN']
        else:
            orgnr_df = fvdf.query(f'ORGNR_FORETAK  == "{nr}" and ORGNR != "{nr}"')['ORGNR']
            navn_df = fvdf.query(f'ORGNR_FORETAK  == "{nr}" and ORGNR != "{nr}"')['NAVN']    

        ant_virksomheter = orgnr_df.shape[0]
        tom_df = pd.Series(np.nan, index=range(ant_kolonner-ant_virksomheter))
        orgnr_df = pd.concat([orgnr_df, tom_df], axis=0)
        orgnr_df.index = orgnr_virk

        ant_navn = navn_df.shape[0]
        tom_df = pd.Series(np.nan, index=range(ant_kolonner-ant_navn))
        navn_df = pd.concat([navn_df, tom_df], axis=0)
        navn_df.index = navn_virk

        foretak_df = pd.concat([orgnr_df, navn_df], axis=0)
        foretak_df['ORGNR_FORETAK'] = nr

        if type(foretak_org_df) == int:                                             # limer sammen hvert foretak med nye kolonner til en stor tabell
            foretak_org_df = foretak_df
        else:
            foretak_org_df = pd.concat([foretak_org_df, foretak_df], axis = 1)
    return foretak_org_df.transpose()


# + [markdown] toc-hr-collapsed=true
# ### skj38O (TSB for offentlige helseforetak)
# -

# Alle offentlige helseforetak foruten ```SUNNAAS```. Bruker ```HF```-tabellen laget i kapittel 2.1.1

# Henter døgnplasser fra foregående år
d_plass_fjor = (skjemadata38O[['FORETAKETS_ORGNR','DGN_DGN']].reset_index().copy()
                [['FORETAKETS_ORGNR','DGN_DGN']]
                .rename(columns={'DGN_DGN': 'D_PLAS_FJOR'}))

skj38O = HF.query('ORGNR_FORETAK != "883971752"').copy()                           # Tar ut SUNNAAS

# +
# Lager kolonneoverskrifter i tråd med tidligere droplister:
navn_virk, orgnr_virk = lag_navn_orgnr_kolonnenavn(20)

onskede_kolonner = ["USERID", "HELSEREGION", "HELSEREGION_NAVN",
                    "FORETAKETS_ORGNR", "FORETAKETS_NAVN",
                    "D_PLAS_FJOR"] + orgnr_virk + navn_virk
# -

# Henter data fra SFU med næringskode ("86.106") og statuskode ("B")
# Næringskode i kolonne SN07_1

skj38O['tmp_bool'] = True

finne_virksomheter_df = pd.merge(SFUklass, skj38O, how="left", on=["ORGNR_FORETAK", "RHF", "NAVN_KLASS", "HELSEREGION"])
finne_virksomheter_df = finne_virksomheter_df.query('tmp_bool == True and SN07_1 == "86.106" and STATUS == "B"')
finne_virksomheter_df = finne_virksomheter_df[['ORGNR','ORGNR_FORETAK','NAVN']]

undervirksomheter_navn_og_kolonner = lag_navn_orgnr_kolonner(finne_virksomheter_df, 20)

# +
skj38O = pd.merge(skj38O, undervirksomheter_navn_og_kolonner, how="left", on="ORGNR_FORETAK")

skj38O['USERID'] = skj38O['ORGNR_FORETAK']
skj38O = skj38O.rename(columns={"ORGNR_FORETAK": "FORETAKETS_ORGNR",
                                "NAVN_KLASS": "FORETAKETS_NAVN",
                                "RHF": "HELSEREGION_NAVN"})

skj38O = pd.merge(skj38O, d_plass_fjor, how="left", on="FORETAKETS_ORGNR")

skj38O = skj38O[onskede_kolonner]
# -

# ### skj44O (Psykisk helsevern for voksne PHFV)

# OBS: Nye enheter 2022

# Alle offentlige helseforetak foruten ```SUNNAAS```. Bruker ```HF```-tabellen laget i kapittel 2.1.1

# Henter døgnplasser fra foregående år
d_plass_fjor = (skjemadata44O[['FORETAKETS_ORGNR','D_PLAS_T']].reset_index().copy()
                [['FORETAKETS_ORGNR','D_PLAS_T']]
                .rename(columns={'D_PLAS_T': 'D_PLAS_FJOR'}))

skj44O = HF.query('ORGNR_FORETAK != "883971752"').copy()                    # Tar ut SUNNAAS

# +
# Lager kolonneoverskrifter i tråd med tidligere .csv-filer:
navn_virk, orgnr_virk = lag_navn_orgnr_kolonnenavn(20)

onskede_kolonner = ["USERID", "HELSEREGION", "HELSEREGION_NAVN",
                    "FORETAKETS_ORGNR", "FORETAKETS_NAVN",
                    "D_PLAS_FJOR"] + orgnr_virk + navn_virk
# -

# Henter data fra SFU med næringskode ("86.104") og statuskode ("B"). Finner næringskoden i kolonne ```SN07_1```

skj44O['tmp_bool'] = True

finne_virksomheter_df = pd.merge(SFUklass, skj44O, how="left", on=["ORGNR_FORETAK", "RHF", "NAVN_KLASS", "HELSEREGION"])
finne_virksomheter_df = finne_virksomheter_df.query('tmp_bool == True and SN07_1 == "86.104" and STATUS == "B"')
finne_virksomheter_df = finne_virksomheter_df[['ORGNR','ORGNR_FORETAK','NAVN']]

undervirksomheter_navn_og_kolonner = lag_navn_orgnr_kolonner(finne_virksomheter_df, 20)

# +
skj44O = pd.merge(skj44O, undervirksomheter_navn_og_kolonner, how="left", on="ORGNR_FORETAK")

skj44O['USERID'] = skj44O['ORGNR_FORETAK']
skj44O['USERID'] = skj44O['ORGNR_FORETAK']
skj44O = skj44O.rename(columns={"ORGNR_FORETAK": "FORETAKETS_ORGNR",
                                "NAVN_KLASS": "FORETAKETS_NAVN",
                                "RHF": "HELSEREGION_NAVN"})

skj44O = pd.merge(skj44O, d_plass_fjor, how="left", on="FORETAKETS_ORGNR")

skj44O = skj44O[onskede_kolonner]
# -

# ### skj45O (Psykisk helsevern for barn og unge (PHBU/BUP), offentlige helseforetak)

# Henter døgnplasser fra foregående år
d_plass_fjor = (skjemadata45O[['FORETAKETS_ORGNR','D_PLAS_T']].reset_index().copy()
                [['FORETAKETS_ORGNR','D_PLAS_T']]
                .rename(columns={'D_PLAS_T': 'D_PLAS_FJOR'}))

skj45O = HF.query('ORGNR_FORETAK != "883971752"').copy()                    # Tar ut SUNNAAS

# +
# Lager kolonneoverskrifter i tråd med tidligere .csv-filer:
navn_virk, orgnr_virk = lag_navn_orgnr_kolonnenavn(20)

onskede_kolonner = ["USERID", "HELSEREGION", "HELSEREGION_NAVN",
                    "FORETAKETS_ORGNR", "FORETAKETS_NAVN",
                    "D_PLAS_FJOR"] + orgnr_virk + navn_virk
# -

# Henter data fra SFU med næringskode ("86.105") og statuskode ("B"). Finner næringskoden i kolonne ```SN07_1```

skj45O['tmp_bool'] = True

finne_virksomheter_df = pd.merge(SFUklass, skj45O, how="left", on=["ORGNR_FORETAK", "RHF", "NAVN_KLASS", "HELSEREGION"])
finne_virksomheter_df = finne_virksomheter_df.query('tmp_bool == True and SN07_1 == "86.105" and STATUS == "B"')
finne_virksomheter_df = finne_virksomheter_df[['ORGNR','ORGNR_FORETAK','NAVN']]

undervirksomheter_navn_og_kolonner = lag_navn_orgnr_kolonner(finne_virksomheter_df, 20)

skj45O = pd.merge(skj45O, undervirksomheter_navn_og_kolonner, how="left", on="ORGNR_FORETAK")

skj45O['USERID'] = skj45O['ORGNR_FORETAK']
skj45O['USERID'] = skj45O['ORGNR_FORETAK']
skj45O = skj45O.rename(columns={"ORGNR_FORETAK": "FORETAKETS_ORGNR",
                                "NAVN_KLASS": "FORETAKETS_NAVN",
                                "RHF": "HELSEREGION_NAVN"})

skj45O = pd.merge(skj45O, d_plass_fjor, how="left", on="FORETAKETS_ORGNR")

skj45O = skj45O[onskede_kolonner]

# ### skj46O (Somatiske sykehus, offentlige helseforetak)
# ```SUNNAAS``` skal være med her
#
# NB. Antall avsatte kolonner til undervirksomheter er 24.

# Henter døgnplasser fra foregående år
d_plass_fjor = (skjemadata46O[['ORGNR_VIRK1','SEN_HT', 'SDGN_HT']].reset_index().copy()
                [['ORGNR_VIRK1','SEN_HT', 'SDGN_HT']]
                .rename(columns={'SEN_HT': 'SEN_HT_FJOR',
                                 'SDGN_HT': 'SDGN_HT_FJOR',
                                 'ORGNR_VIRK1': 'FINST_ORGNR'}))

skj46O = HF.copy()

# +
# Lager kolonneoverskrifter i tråd med tidligere .csv-filer:
navn_virk, orgnr_virk = lag_navn_orgnr_kolonnenavn(24)

kolonner = ["USERID", "HELSEREGION", "HELSEREGION_NAVN",
            "FORETAKETS_ORGNR", "FORETAKETS_NAVN",
            'SEN_HT_FJOR', 'SDGN_HT_FJOR'] + orgnr_virk + navn_virk
# -

# Henter data fra SFU med næringskoder ("86.101") ("86.102") ("86.103") ("86.107"). Finner næringskoden i kolonne ```SN07_1```

# +
skj46O['tmp_bool'] = True

finne_virksomheter_df = pd.merge(SFUklass, skj46O, how="left", on=["ORGNR_FORETAK", "RHF", "NAVN_KLASS", "HELSEREGION"])
finne_virksomheter_df = finne_virksomheter_df.query('tmp_bool == True and SN07_1 in ["86.101", "86.102", "86.103","86.107",]')

finne_virksomheter_df = finne_virksomheter_df[['ORGNR','ORGNR_FORETAK','NAVN', 'NAVN_KLASS']]

finne_virksomheter_df = pd.merge(finne_virksomheter_df, d_plass_fjor, how="left", left_on="ORGNR", right_on="FINST_ORGNR")

finne_virksomheter_df['SEN_HT_FJOR'] = pd.to_numeric(finne_virksomheter_df['SEN_HT_FJOR'])
finne_virksomheter_df['SDGN_HT_FJOR'] = pd.to_numeric(finne_virksomheter_df['SDGN_HT_FJOR'])

finne_virksomheter_df = finne_virksomheter_df.groupby(["ORGNR_FORETAK", "NAVN_KLASS"]).sum(numeric_only=True).reset_index()
finne_virksomheter_df = finne_virksomheter_df.rename(columns={"NAVN_KLASS": "FORETAKETS_NAVN"})

# +
finne_virksomheter_df2 = pd.merge(SFUklass, skj46O, how="left", on=["ORGNR_FORETAK", "RHF", "NAVN_KLASS", "HELSEREGION"])
finne_virksomheter_df2 = finne_virksomheter_df2.query('tmp_bool == True and SN07_1 in ["86.101", "86.102", "86.103","86.107",]')

finne_virksomheter_df2 = finne_virksomheter_df2[['ORGNR','ORGNR_FORETAK','NAVN', 'NAVN_KLASS']]

# +
undervirksomheter_navn_og_kolonner = lag_navn_orgnr_kolonner(finne_virksomheter_df2, 24, False)

skj46O = pd.merge(skj46O, undervirksomheter_navn_og_kolonner, how="left", on="ORGNR_FORETAK")

skj46O = pd.merge(skj46O, finne_virksomheter_df, how="left", on="ORGNR_FORETAK")

skj46O['USERID'] = skj46O['ORGNR_FORETAK']
skj46O = skj46O.rename(columns={"ORGNR_FORETAK": "FORETAKETS_ORGNR",
                                "NAVN_KLASS": "FORETAKETS_NAVN",
                                "RHF": "HELSEREGION_NAVN"})
# -

skj46O = skj46O[kolonner]


# + [markdown] toc-hr-collapsed=true
# ## Skjemaer til private foretak og deres underinstitusjoner
# Droplisten inneholder kolonne med rapporteringspliktige underinstitusjoner i en kolonne atskilt med \n
# -

# #### Nyttige funksjoner som skal gjøre koden mer lesbar

def hent_foretaksnavn_til_virksomhetene_fra_SFU(unike_foretak):
    unike_foretak = pd.DataFrame(unike_foretak)
    unike_foretak.columns = ["ORGNR_FORETAK"]

    finn_foretaksnavn = pd.merge(unike_foretak, SFUklass[["ORGNR", "NAVN"]],
                                 how="left",
                                 left_on="ORGNR_FORETAK",
                                 right_on="ORGNR")
    finn_foretaksnavn = finn_foretaksnavn[["ORGNR_FORETAK", "NAVN"]]\
                                        .rename(columns={"NAVN": "FORETAK_NAVN",
                                        "ORGNR_FORETAK": "FORETAK_ORGNR"})
    return finn_foretaksnavn


# Lager kolonnen INSTLIST med riktig antall \n for at det skal passe med skjema. Passer på at rapporteringsenheten kommer først i listen. (Enhet som har H_VAR1_A lik orgnummer)

def instlist_med_riktig_antall_n(finst_orgnr_df):
    df_til_instlist = pd.merge(SFUklass, finst_orgnr_df, left_on="H_VAR1_A", right_on="FINST_ORGNR")
    df_til_instlist['rapporterer_til_annen_virksomhet'] = (df_til_instlist.ORGNR != df_til_instlist.H_VAR1_A)
    df_til_instlist = df_til_instlist[['NAVN', 'ORGNR', "FINST_ORGNR", 'rapporterer_til_annen_virksomhet']]
    df_til_instlist['orgnr_navn'] = df_til_instlist['ORGNR'] + " - " + df_til_instlist['NAVN']

    rapporterer_ikke_til_annen_v = df_til_instlist.query('~rapporterer_til_annen_virksomhet')[['ORGNR', 'orgnr_navn']]
    rapporterer_til_annen_v = df_til_instlist.query('rapporterer_til_annen_virksomhet')[["FINST_ORGNR", 'orgnr_navn']]
    
    # mellomsteg for å legge institusjonene horisontalt ved siden av rapporteringsnummeret
    # itererer over alle unike rapporteringsnummer
    tmpdf = -1
    for finst_orgnr in rapporterer_ikke_til_annen_v.ORGNR.unique():
        instliste = "\\n".join(rapporterer_til_annen_v.query(f'FINST_ORGNR=="{finst_orgnr}"').orgnr_navn)
        if len(instliste) > 0:
            if type(tmpdf) == int:
                tmpdf = pd.DataFrame([finst_orgnr, instliste])
            else:
                nytt_tillegg = pd.DataFrame([finst_orgnr, instliste])
                tmpdf = pd.concat([tmpdf, nytt_tillegg], axis=1)
    
    if type(tmpdf) != int:
        tmpdf = tmpdf.transpose()
        tmpdf.columns = ['FINST_ORGNR', 'INSTLISTE_HALE']   
    
    rapporterer_ikke_til_annen_v = rapporterer_ikke_til_annen_v.rename(columns={'ORGNR': 'FINST_ORGNR'})
    return rapporterer_ikke_til_annen_v, tmpdf


def legg_paa_hale_med_n(df):
    df['INSTLISTE_HALE'] = df['INSTLISTE_HALE'].fillna("\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n")
    df['antall_n_mangler'] = df['INSTLISTE_HALE'].apply(lambda x: 11-x.count("\\n"))
    df['INSTLIST'] = df.apply(lambda x: x.orgnr_navn + \
                                                "\\n" + \
                                                x.INSTLISTE_HALE + \
                                                x.antall_n_mangler * ("\\n"), axis=1)
    return df


kolonner_i_alle_private = ['USERID',
                            'REGION_NR',
                            'REGION_NAVN',
                            'FORETAK_ORGNR',
                            'FORETAK_NAVN',
                            'FINST_ORGNR',
                            'FINST_NAVN']

# ### skj38P (TSB for private helseforetak)

# Henter inn alle rader i SFU som har skjematype 381. Variabelen ```H_VAR1_A```  sier hvilket orgnummer foretaket rapporterer til. Koden under bruker denne til å variabelen til å plassere rapporteringsenhetene først, og alle deres underinstitusjoner i variabelen ```INSTLIST```. 
#
# For at filen skal ha riktig format, skal det tilsammen være 13 institusjoner per foretak i ```INSTLIST```. Fyller automatisk ut linjeskift ```\n``` slik at antallet blir 13.

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private +\
            ['INSTLIST']

skj38P = tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "381").copy()
skj38P = skj38P.rename(columns = {"ORGNR_FORETAK": "FORETAK_ORGNR",
                                   "NAVN": "FINST_NAVN",
                                   'ORGNR':'FINST_ORGNR',
                                   'HELSEREGION':'REGION_NR',
                                   'RHF':'REGION_NAVN'})

# USERID er alltid foretaksnummer
skj38P['USERID'] = skj38P['FORETAK_ORGNR']


# +
# Importerer riktig regionnummer fra KLASS og gir foretak som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj38P = pd.merge(skj38P, RHF_kode_klass[["HELSEREGION", "RHF"]], how="left" , left_on="REGION_NR", right_on="HELSEREGION")

skj38P['REGION_NAVN'] = skj38P['RHF']
skj38P.REGION_NAVN = skj38P.REGION_NAVN.fillna("PRIVATE INSTITUSJONER")
# -

foretaksnavn = hent_foretaksnavn_til_virksomhetene_fra_SFU(skj38P.FORETAK_ORGNR.unique())
skj38P = pd.merge(skj38P, foretaksnavn, how="left", on="FORETAK_ORGNR")

rapporteringsenhet, undervirksomheter = instlist_med_riktig_antall_n(pd.DataFrame(skj38P['FINST_ORGNR']))

# +
skj38P = pd.merge(skj38P, rapporteringsenhet, how="left", on="FINST_ORGNR")
skj38P = pd.merge(skj38P, undervirksomheter, how="left", on="FINST_ORGNR")

skj38P = legg_paa_hale_med_n(skj38P)
# -

skj38P = skj38P[kolonner]

# ### skj39 (Resultatregnskap for private helseforetak)

# Lages på nesten samme måte som skjema 38P, med unntak av at man henter ut SKJEMA_TYPE 39 fra SFU og legger til kolonnen ```INSTTYPE``` med institusjonstype hentet fra hjelpevariabelen ```SN07_1```.

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private +\
           ['INSTTYPE', 'INSTLIST']

skj39 = tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "39").copy()
skj39 = skj39.rename(columns = {"ORGNR_FORETAK": "FORETAK_ORGNR",
                                   "NAVN": "FINST_NAVN",
                                   'ORGNR':'FINST_ORGNR',
                                   'HELSEREGION':'REGION_NR',
                                   'RHF':'REGION_NAVN'})

# USERID er alltid foretaksnummer
skj39['USERID'] = skj39['FORETAK_ORGNR']


# +
# Importerer riktig regionnummer fra KLASS og gir foretak som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj39 = pd.merge(skj39, RHF_kode_klass[["HELSEREGION", "RHF"]], how="left" , left_on="REGION_NR", right_on="HELSEREGION")

skj39['REGION_NAVN'] = skj39['RHF']
skj39.REGION_NAVN = skj39.REGION_NAVN.fillna("PRIVATE INSTITUSJONER")

# +
# Henter foretaksnavn til virksomhetene fra SFU
foretak = hent_foretaksnavn_til_virksomhetene_fra_SFU(skj39.FORETAK_ORGNR.unique())

skj39 = pd.merge(skj39, foretak, how="left", on="FORETAK_ORGNR")
# -

rapporteringsenhet, undervirksomheter = instlist_med_riktig_antall_n(pd.DataFrame(skj39['FINST_ORGNR']))

skj39 = pd.merge(skj39, rapporteringsenhet, how="left", on="FINST_ORGNR")
skj39 = pd.merge(skj39, undervirksomheter, how="left", on="FINST_ORGNR")
skj39 = legg_paa_hale_med_n(skj39)

# +
institusjonstype = {
    451: 'Psykisk helsevern for barn og unge',
    461: 'Somatiske sykehus',
    381: 'Rusmiddelinstitusjoner',
    441: 'Psykisk helsevern for voksne',
    47:  'Somatiske rehab.-og opptr.inst.'
}

skj39 = skj39.rename(columns={'H_VAR2_N': 'INSTTYPE'})
# -

skj39['INSTTYPE'] = skj39['INSTTYPE'].map(institusjonstype)

# Hvis FORETAK_NAVN er tom, bruk NAVN1 fra SFU:
skj39.loc[skj39['FORETAK_NAVN'].isnull(),'FORETAK_NAVN'] = skj39['NAVN1']

# Tar kun vare på de kolonnene jeg spesifiserer i begynnelsen
skj39 = skj39[kolonner]

# ### skj44P (Psykisk helsevern for voksne (PHFV), private helseforetak)

# Henter døgnplasser fra foregående år
d_plass_fjor = (skjemadata44P[['FINST_ORGNR','D_PLAS_T']].reset_index().copy()
                [['FINST_ORGNR','D_PLAS_T']]
                .rename(columns={'D_PLAS_T': 'D_PLAS_FJOR'}))

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private +\
           ['INSTLIST', 'D_PLAS_FJOR']

skj44P = tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "441").copy()
skj44P = skj44P.rename(columns = {"ORGNR_FORETAK": "FORETAK_ORGNR",
                                   "NAVN": "FINST_NAVN",
                                   'ORGNR':'FINST_ORGNR',
                                   'HELSEREGION':'REGION_NR',
                                   'RHF':'REGION_NAVN'})

# USERID er alltid foretaksnummer
skj44P['USERID'] = skj44P['FORETAK_ORGNR']


# +
# Importerer riktig regionnummer fra KLASS og gir foretak 
# som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj44P = pd.merge(skj44P, RHF_kode_klass[["HELSEREGION", "RHF"]], how="left" , left_on="REGION_NR", right_on="HELSEREGION")

skj44P['REGION_NAVN'] = skj44P['RHF']
skj44P.REGION_NAVN = skj44P.REGION_NAVN.fillna("PRIVATE INSTITUSJONER")

# +
# Henter foretaksnavn til virksomhetene fra SFU
foretak = hent_foretaksnavn_til_virksomhetene_fra_SFU(skj44P.FORETAK_ORGNR.unique())

skj44P = pd.merge(skj44P, foretak, how="left", on="FORETAK_ORGNR")
# -

rapporteringsenhet, undervirksomheter = instlist_med_riktig_antall_n(pd.DataFrame(skj44P['FINST_ORGNR']))

skj44P = pd.merge(skj44P, rapporteringsenhet, how="left", on="FINST_ORGNR")
skj44P = pd.merge(skj44P, undervirksomheter, how="left", on="FINST_ORGNR")
skj44P = legg_paa_hale_med_n(skj44P)

skj44P = pd.merge(skj44P, d_plass_fjor, how="left", on="FINST_ORGNR")

# Tar kun vare på de kolonnene jeg spesifiserte i begynnelsen 
skj44P = skj44P[kolonner]

# ### skj45P (Psykisk helsevern for barn og unge (PHBU/BUP), private helseforetak)

# Henter døgnplasser fra foregående år
d_plass_fjor = (skjemadata45P[['FINST_ORGNR','D_PLAS_T']].reset_index().copy()
                [['FINST_ORGNR','D_PLAS_T']]
                .rename(columns={'D_PLAS_T': 'D_PLAS_FJOR'}))

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private +\
           ['INSTLIST', 'D_PLAS_FJOR']

skj45P = tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "451").copy()
skj45P = skj45P.rename(columns = {"ORGNR_FORETAK": "FORETAK_ORGNR",
                                   "NAVN": "FINST_NAVN",
                                   'ORGNR':'FINST_ORGNR',
                                   'HELSEREGION':'REGION_NR',
                                   'RHF':'REGION_NAVN'})

# USERID er alltid foretaksnummer
skj45P['USERID'] = skj45P['FORETAK_ORGNR']

# +
# Importerer riktig regionnummer fra KLASS og gir foretak 
# som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj45P = pd.merge(skj45P, RHF_kode_klass[["HELSEREGION", "RHF"]], how="left" , left_on="REGION_NR", right_on="HELSEREGION")

skj45P['REGION_NAVN'] = skj45P['RHF']
skj45P.REGION_NAVN = skj45P.REGION_NAVN.fillna("PRIVATE INSTITUSJONER")

# +
# Henter foretaksnavn til virksomhetene fra SFU
foretak = hent_foretaksnavn_til_virksomhetene_fra_SFU(skj45P.FORETAK_ORGNR.unique())

skj45P = pd.merge(skj45P, foretak, how="left", on="FORETAK_ORGNR")
# -

rapporteringsenhet, undervirksomheter = instlist_med_riktig_antall_n(pd.DataFrame(skj45P['FINST_ORGNR']))

# +
skj45P = pd.merge(skj45P, rapporteringsenhet, how="left", on="FINST_ORGNR")
if type(undervirksomheter) != int:
    skj45P = pd.merge(skj45P, undervirksomheter, how="left", on="FINST_ORGNR")
    skj45P['INSTLISTE_HALE'] = skj45P['INSTLISTE_HALE'].fillna("\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n")
else:
    skj45P['INSTLISTE_HALE'] = "\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n"


skj45P = legg_paa_hale_med_n(skj45P)
# -

skj45P = pd.merge(skj45P, d_plass_fjor, how="left", on="FINST_ORGNR")

# Tar kun vare på de kolonnene spesifisert i begynnelsen
skj45P = skj45P[kolonner]

# ### skj46P (Somatiske sykehus, private helseforetak)

# Henter døgnplasser fra foregående år
d_plass_fjor = (skjemadata46P[['FINST_ORGNR','SEN_HT', 'SDGN_HT']].reset_index().copy()
                [['FINST_ORGNR','SEN_HT', 'SDGN_HT']]
                .rename(columns={'SEN_HT': 'SEN_HT_FJOR',
                                 'SDGN_HT': 'SDGN_HT_FJOR'}))

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private +\
            ['SEN_HT_FJOR', 'SDGN_HT_FJOR']

skj46P = tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "461").copy()
skj46P = skj46P.rename(columns = {"ORGNR_FORETAK": "FORETAK_ORGNR",
                                   "NAVN": "FINST_NAVN",
                                   'ORGNR':'FINST_ORGNR',
                                   'HELSEREGION':'REGION_NR',
                                   'RHF':'REGION_NAVN'})

# USERID er alltid foretaksnummer
skj46P['USERID'] = skj46P['FORETAK_ORGNR']


# +
# Importerer riktig regionnummer fra KLASS og gir foretak som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj46P = pd.merge(skj46P, RHF_kode_klass[["HELSEREGION", "RHF"]], how="left" , left_on="REGION_NR", right_on="HELSEREGION")

skj46P['REGION_NAVN'] = skj46P['RHF']
skj46P.REGION_NAVN = skj46P.REGION_NAVN.fillna("PRIVATE INSTITUSJONER")

# +
# Henter foretaksnavn til virksomhetene fra SFU
foretak = hent_foretaksnavn_til_virksomhetene_fra_SFU(skj46P.FORETAK_ORGNR.unique())

skj46P = pd.merge(skj46P, foretak, how="left", on="FORETAK_ORGNR")
# -

rapporteringsenhet, undervirksomheter = instlist_med_riktig_antall_n(pd.DataFrame(skj46P['FINST_ORGNR']))

skj46P = pd.merge(skj46P, rapporteringsenhet, how="left", on="FINST_ORGNR")

# +
skj46P = pd.merge(skj46P, undervirksomheter, how="left", on="FINST_ORGNR")
skj46P.orgnr_navn = skj46P.orgnr_navn.fillna("")

skj46P = legg_paa_hale_med_n(skj46P)
# -

skj46P = pd.merge(skj46P, d_plass_fjor, how="left", on="FINST_ORGNR")

skj46P.loc[skj46P.FORETAK_NAVN.isnull(), 'FORETAK_NAVN'] = skj46P.NAVN1

# Tar kun vare på de kolonnene jeg spesifiserte i begynnelsen 
skj46P = skj46P[kolonner]

# ### skj47 (Somatiske institusjoner)

# Henter døgnplasser fra foregående år
d_plass_fjor = (skjemadata47[['FINST_ORGNR','D_HELD', 'SDGN_SUM']].reset_index().copy()
                [['FINST_ORGNR','D_HELD', 'SDGN_SUM']]
                .rename(columns={'D_HELD': 'D_HELD_IFJOR',
                                 'SDGN_SUM': 'SDGN_SUM_IFJOR'}))

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private +\
           ['D_HELD_IFJOR','SDGN_SUM_IFJOR']

skj47 = tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "47").copy()
skj47 = skj47.rename(columns = {"ORGNR_FORETAK": "FORETAK_ORGNR",
                                   "NAVN": "FINST_NAVN",
                                   'ORGNR':'FINST_ORGNR',
                                   'HELSEREGION':'REGION_NR',
                                   'RHF':'REGION_NAVN'})

# USERID er alltid foretaksnummer
skj47['USERID'] = skj47['FORETAK_ORGNR']

# +
# Importerer riktig regionnummer fra KLASS og gir foretak som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj47 = pd.merge(skj47, RHF_kode_klass[["HELSEREGION", "RHF"]], how="left" , left_on="REGION_NR", right_on="HELSEREGION")

skj47['REGION_NAVN'] = skj47['RHF']
skj47.REGION_NAVN = skj47.REGION_NAVN.fillna("PRIVATE INSTITUSJONER")
# -

# Henter foretaksnavn til virksomhetene fra SFU
foretak = hent_foretaksnavn_til_virksomhetene_fra_SFU(skj47.FORETAK_ORGNR.unique())
skj47 = pd.merge(skj47, foretak, how="left", on="FORETAK_ORGNR")

rapporteringsenhet, undervirksomheter = instlist_med_riktig_antall_n(pd.DataFrame(skj47['FINST_ORGNR']))

skj47 = pd.merge(skj47, rapporteringsenhet, how="left", on="FINST_ORGNR")
skj47 = pd.merge(skj47, undervirksomheter, how="left", on="FINST_ORGNR")
skj47 = legg_paa_hale_med_n(skj47)

skj47 = pd.merge(skj47, d_plass_fjor, how="left", on="FINST_ORGNR")

# Hvis FORETAK_NAVN er tom, bruk NAVN1 fra SFU:
skj47.loc[skj47['FORETAK_NAVN'].isnull(),'FORETAK_NAVN'] = skj47['NAVN1']

# Tar kun vare på de kolonnene jeg spesifiserte i begynnelsen
skj47 = skj47[kolonner]


# # Eksportering til .csv-filer og rapport

# +
def rapport(skj):
    rader = eval(skj).shape[0]
    kolonner = eval(skj).shape[1]
    rader_med_missing = rader - eval(skj).dropna().shape[0]
    skl = 100 * "-"
    s = f"{skl}\n{skj}\t Rader: {rader} \t Kolonner: {kolonner} \t Rader som inneholder minst en missing value: {rader_med_missing} "
    return s

def lagre_dropliste_csv(skj):
    filnavn = "Dropliste_" + skj + "_" + str(aargang) + "_" + dato_idag + ".csv"
    eval(skj).to_csv(sti_til_lagring + filnavn, sep=";", encoding='latin1', index=False)
    print(sti_til_lagring + filnavn, " lagret")


# -

for x in skjemaer_til_droplister:
    print(rapport(x))

if lagre_filer:
    for x in skjemaer_til_droplister:
        lagre_dropliste_csv(x)


