# -*- coding: utf-8 -*-
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
#   skj48         |  HELSE48         |  Samhandlingsleger                            

# + [markdown] toc-hr-collapsed=true
# # Setup
# -

# ## Importerer pakker og setter innstillinger

# +
import pandas as pd

from sqlalchemy import create_engine

import getpass
import os

import hjelpefunksjoner as hjfunk

# +
# Fjerner begrensning på antall rader og kolonner som vises av gangen
pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 50)
pd.set_option('display.max_colwidth', None)

# Unngå standardform i output
pd.set_option('display.float_format', lambda x: '%.0f' % x)
# -

# ## Velger årgang og delregister

# NB! For å lagre filene til angitt sti må `lagre_filer` settes til `True`.

# +
aargang = 2024
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

username = getpass.getuser()
dsn = "DB1P"
try:
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")
except:
    print("Passord ikke skrevet inn")
    password = getpass.getpass(prompt='Oracle-passord: ')
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")

# Opprett en tilkobling fra motoren
conn = engine.connect()

# # Henter data

# + [markdown] toc-hr-collapsed=true
# ##  fra KLASS

# +
HF, RHF, phob, rfss, rfss2, rfss3, rapporteringsenheter = hjfunk.hent_enheter_fra_klass(
    aargang
)

regionoppslag = rapporteringsenheter[
    rapporteringsenheter["NAVN_KLASS"].str.endswith("RHF")
].copy()[["HELSEREGION", "HELSEREGION_NAVN"]]
# -

print(" Rapporteringsenheter fra KLASS årgang", aargang, "1. januar")
print(55*"-")
print(f"HF: \t\t\t\t\t\t\t\t\t{HF.shape[0]} enheter")
print(f"RHF: \t\t\t\t\t\t\t\t\t{RHF.shape[0]} enheter")
print(f"Private helseforetak med oppdrag og bestillerdokument: \t\t\t{phob.shape[0]} enheter")
print(f"Regionale og felleseide støtteforetak i spesialisthelsetjenesten: \t{rfss.shape[0]+rfss2.shape[0]+rfss3.shape[0]} enheter")
print(f"Rapporteringsenheter: \t\t\t\t\t\t\t{rapporteringsenheter.shape[0]} enheter")

# ## VOF: liste over alle helseforetak

rapporteringsenheter_uten_RHF = rapporteringsenheter[
    ~rapporteringsenheter["NAVN_KLASS"].str.endswith("RHF")
]

r_orgnr = rapporteringsenheter_uten_RHF.ORGNR_FORETAK.to_numpy()

# +
sql_str = hjfunk.lag_sql_str(r_orgnr)

sporring_for = f"""
    SELECT FORETAKS_NR, ORGNR, NAVN
    FROM DSBBASE.SSB_FORETAK
    WHERE STATUSKODE = 'B' AND ORGNR IN {sql_str}
"""
vof_for = hjfunk.les_sql(sporring_for, conn)

# +
fornummer = pd.Series(vof_for["FORETAKS_NR"]).array
sql_str = hjfunk.lag_sql_str(fornummer)

sporring_bed = f"""
    SELECT FORETAKS_NR, ORGNR, NAVN, KARAKTERISTIKK, SN07_1, SB_TYPE
    FROM DSBBASE.SSB_BEDRIFT
    WHERE STATUSKODE = 'B' AND FORETAKS_NR IN {sql_str}
"""
vof_bdr = hjfunk.les_sql(sporring_bed, conn)
# -

# Henter organisasjons- og foretaksnummer fra Virksomhets- og foretaksregisteret (VoF) og samler disse i én tabell kalt ```vof```

# +
vof_for = vof_for.rename(columns={"NAVN": "NAVN_FORETAK",
                                  "ORGNR": "ORGNR_FORETAK"})

vof_bdr = vof_bdr.rename(columns={"ORGNR": "ORGNR_BEDRIFT"})
vof_bdr["KARAKTERISTIKK"] = vof_bdr["KARAKTERISTIKK"].fillna("")
vof_bdr["NAVN_BEDRIFT"] = vof_bdr["NAVN"] + " " + vof_bdr["KARAKTERISTIKK"]

vof_bdr = vof_bdr.drop(columns=["NAVN", "KARAKTERISTIKK"])

vof = pd.merge(vof_bdr, vof_for, how="left", on="FORETAKS_NR")
vof = vof.drop(columns=["FORETAKS_NR"])

rapporteringsenheter["ORGNR_FORETAK"] = rapporteringsenheter["ORGNR_FORETAK"].apply(str)
rapporteringsenheter_vof = pd.merge(
    vof, rapporteringsenheter, how="left", on="ORGNR_FORETAK"
)
# -

# ## Dynarev

# Henter ut SFU og tar senere vare på de som har har non-missing på ```SKJEMA_TYPE```.

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{siste_to_siffer_aargang}')
"""
SFU_data = hjfunk.les_sql(sporring, conn)
print(f"Rader:    {SFU_data.shape[0]}\nKolonner: {SFU_data.shape[1]}")
SFU_data.info()


# Enheter med verdi i 'KVITT_TYPE' filtreres ut (de er nedlagte enheter). Fjerner også enheter uten ORGNR

SFU_data = SFU_data[~SFU_data['KVITT_TYPE'].notnull()]
SFU_data = SFU_data[SFU_data['ORGNR'].notnull()]

# +
skjemanavn_liste = [
    "HELSE38O", "HELSE38P", "HELSE39", "HELSE40",
    "HELSE41", "HELSE44O", "HELSE44P", "HELSE45O",
    "HELSE45P", "46O", "HELSE46P", "HELSE47", "HELSE48",
]

skjemadata = {}
for navn in skjemanavn_liste:
    skjemadata[navn] = hjfunk.hent_data_delreg24x_og_19377x(
        siste_to_siffer_aar_for, navn, conn
    )
# -

# # Setter sammen master-SFU-fil med all nødvendig informasjon

# SFU: orgnummer, orgnummer_foretak, foretaksnavn, helseregionnavn, helseregionsnummer

SFUklass = pd.merge(SFU_data, rapporteringsenheter, how="left", on="ORGNR_FORETAK")

# +
SFUklass["HELSEREGION"] = SFUklass["HELSEREGION"].fillna("06")

SFUklass["SKJEMA_TYPE"] = SFUklass["SKJEMA_TYPE"].apply(lambda x: str(x).split(" "))
SFUklass[["NAVN1", "NAVN2", "NAVN3", "NAVN4", "NAVN5"]] = SFUklass[
    ["NAVN1", "NAVN2", "NAVN3", "NAVN4", "NAVN5"]
].fillna("")
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
skjemaer_til_droplister = ["skj0X", "skj0Y", "skj38O", "skj38P", "skj39", "skj39b",
                           "skj40", "skj41", "skj44O", "skj44P", "skj45O",
                           "skj45P", "skj46O", "skj46P", "skj46Pb", "skj47", "skj48"]

# ## Skjemaer som hentes rett fra klass og SFU

# ### 0X (Resultatregnskap for RHF og HF)

# +
# kolonner som skal være med i droplisten:
kolonner0X0Y404148 = [
    "USERID", "REGION_NR", "REGION_NAVN",
    "FORETAK_ORGNR", "FORETAK_NAVN",
]

skj0X = hjfunk.tabell_som_inneholder_skjema(SFUklass, "SKJEMA_TYPE", "0X")
skj0X = skj0X.rename(
    columns={
        "NAVN1": "FORETAK_NAVN",
        "ORGNR": "USERID",
        "HELSEREGION": "REGION_NR",
        "HELSEREGION_NAVN": "REGION_NAVN",
    }
)
skj0X["FORETAK_ORGNR"] = skj0X["USERID"]

skj0X = skj0X[kolonner0X0Y404148].sort_values("REGION_NR")
# -

# ### 0Y (Balanseregnskap for RHF og HF)

# +
skj0Y = hjfunk.tabell_som_inneholder_skjema(SFUklass, "SKJEMA_TYPE", "0Y")
skj0Y = skj0Y.rename(
    columns={
        "NAVN1": "FORETAK_NAVN",
        "ORGNR": "USERID",
        "HELSEREGION": "REGION_NR",
        "HELSEREGION_NAVN": "REGION_NAVN",
    }
)
skj0Y["FORETAK_ORGNR"] = skj0Y["USERID"]

skj0Y = skj0Y[kolonner0X0Y404148].sort_values("REGION_NR")
# -

# ### skj40 (Kontantstrømoppstilling)

# +
skj40 = hjfunk.tabell_som_inneholder_skjema(SFUklass, "SKJEMA_TYPE", "0Y")
skj40 = skj40.rename(
    columns={
        "NAVN1": "FORETAK_NAVN",
        "ORGNR": "USERID",
        "HELSEREGION": "REGION_NR",
        "HELSEREGION_NAVN": "REGION_NAVN",
    }
)
skj40["FORETAK_ORGNR"] = skj40["USERID"]

skj40 = skj40[kolonner0X0Y404148].sort_values("REGION_NR")
# -

# ### skj41 (Private spesialister med driftsavtale)

skj41 = rapporteringsenheter[rapporteringsenheter['NAVN_KLASS'].str.endswith("RHF")].copy()

skj41 = skj41.rename(
    columns={
        "HELSEREGION": "REGION_NR",
        "NAVN_KLASS": "FORETAK_NAVN",
        "HELSEREGION_NAVN": "REGION_NAVN",
        "ORGNR_FORETAK": "FORETAK_ORGNR",
    }
)
skj41["USERID"] = skj41["FORETAK_ORGNR"]
skj41 = skj41[kolonner0X0Y404148]

# ### skj48 (Praksiskonsulentordningen)

skj48 = hjfunk.tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "48").copy()
skj48 = skj48[["NAVN_KLASS", "ORGNR_FORETAK", "HELSEREGION"]]

skj48 = pd.concat([skj48, HF])

skj48 = pd.merge(skj48, regionoppslag, how="left", on="HELSEREGION")

# +
skj48 = skj48.rename(
    columns={
        "NAVN_KLASS": "FORETAK_NAVN",
        "ORGNR_FORETAK": "FORETAK_ORGNR",
        "HELSEREGION": "REGION_NR",
        "HELSEREGION_NAVN": "REGION_NAVN",
    }
)
skj48["USERID"] = skj48["FORETAK_ORGNR"]

skj48 = skj48[kolonner0X0Y404148].sort_values("REGION_NR").reset_index(drop=True)
# -

# ## Skjemaer til offentlige foretak
# Inneholder et visst antall kolonner med virksomheters navn og orgnr.

# + [markdown] toc-hr-collapsed=true
# ### skj38O (TSB for offentlige helseforetak)
# -

# Alle offentlige helseforetak foruten ```SUNNAAS```. Bruker ```HF```-tabellen laget i kapittel 2.1.1

# Henter døgnplasser fra foregående år
d_plass_fjor = (
    skjemadata["HELSE38O"][["FORETAKETS_ORGNR", "DGN_DGN"]]
    .reset_index()
    .copy()[["FORETAKETS_ORGNR", "DGN_DGN"]]
    .rename(columns={"DGN_DGN": "D_PLAS_FJOR"})
)

skj38O = HF[HF['ORGNR_FORETAK'] != "883971752"].copy()                           # Tar ut SUNNAAS

# +
# Lager kolonneoverskrifter i tråd med tidligere droplister:
navn_virk, orgnr_virk = hjfunk.lag_navn_orgnr_kolonnenavn(20)

onskede_kolonner = ["USERID", "HELSEREGION", "HELSEREGION_NAVN",
                    "FORETAKETS_ORGNR", "FORETAKETS_NAVN",
                    "D_PLAS_FJOR"] + orgnr_virk + navn_virk
# -

# Henter data fra SFU med næringskode ("86.106") og statuskode ("B"). Næringskode i kolonne SN07_1

temp = pd.merge(
    skj38O["ORGNR_FORETAK"],
    SFUklass[
        (SFUklass["SN07_1"] == "86.106") &
        (SFUklass["STATUS"] == "B")]
    [["ORGNR", "ORGNR_FORETAK", "NAVN"]],
    how="left",
    on="ORGNR_FORETAK",
).dropna(subset=["ORGNR"])

temp = hjfunk.lag_navn_orgnr_kolonner(temp, 20)

skj38O = pd.merge(skj38O, temp, how="left", on="ORGNR_FORETAK")

# +
skj38O['USERID'] = skj38O['ORGNR_FORETAK']
skj38O = skj38O.rename(columns={"ORGNR_FORETAK": "FORETAKETS_ORGNR",
                                "NAVN_KLASS": "FORETAKETS_NAVN"
                                })

skj38O = pd.merge(skj38O, d_plass_fjor, how="left", on="FORETAKETS_ORGNR")
skj38O = pd.merge(skj38O, regionoppslag, how="left", on="HELSEREGION")
# -

skj38O = skj38O[onskede_kolonner]



# ### skj44O (Psykisk helsevern for voksne PHFV)

# Alle offentlige helseforetak foruten ```SUNNAAS```. Bruker ```HF```-tabellen laget i kapittel 2.1.1

# Henter døgnplasser fra foregående år
d_plass_fjor = (skjemadata["HELSE44O"][['FORETAKETS_ORGNR', 'D_PLAS_T']].reset_index().copy()
                [['FORETAKETS_ORGNR', 'D_PLAS_T']]
                .rename(columns={'D_PLAS_T': 'D_PLAS_FJOR'}))

skj44O = HF.query('ORGNR_FORETAK != "883971752"').copy()                    # Tar ut SUNNAAS

# +
# Lager kolonneoverskrifter i tråd med tidligere .csv-filer:
navn_virk, orgnr_virk = hjfunk.lag_navn_orgnr_kolonnenavn(20)

onskede_kolonner = ["USERID", "HELSEREGION", "HELSEREGION_NAVN",
                    "FORETAKETS_ORGNR", "FORETAKETS_NAVN",
                    "D_PLAS_FJOR"] + orgnr_virk + navn_virk
# -

# Henter data fra SFU med næringskode ("86.104") og statuskode ("B"). Finner næringskoden i kolonne ```SN07_1```

skj44O['tmp_bool'] = True

finne_virksomheter_df = pd.merge(
    SFUklass, skj44O, how="left", on=["ORGNR_FORETAK", "NAVN_KLASS", "HELSEREGION"]
)
finne_virksomheter_df = finne_virksomheter_df.query(
    'tmp_bool == True and SN07_1 == "86.104" and STATUS == "B"'
)
finne_virksomheter_df = finne_virksomheter_df[["ORGNR", "ORGNR_FORETAK", "NAVN"]]

undervirksomheter_navn_og_kolonner = hjfunk.lag_navn_orgnr_kolonner(finne_virksomheter_df, 20)

# +
skj44O = pd.merge(
    skj44O, undervirksomheter_navn_og_kolonner, how="left", on="ORGNR_FORETAK"
)

skj44O["USERID"] = skj44O["ORGNR_FORETAK"]
skj44O["USERID"] = skj44O["ORGNR_FORETAK"]
skj44O = skj44O.rename(
    columns={"ORGNR_FORETAK": "FORETAKETS_ORGNR",
             "NAVN_KLASS": "FORETAKETS_NAVN"}
)

skj44O = pd.merge(skj44O, d_plass_fjor, how="left", on="FORETAKETS_ORGNR")
# -

skj44O = pd.merge(skj44O, regionoppslag, how="left", on="HELSEREGION")

skj44O = skj44O[onskede_kolonner]

# ### skj45O (Psykisk helsevern for barn og unge (PHBU/BUP), offentlige helseforetak)

# Henter døgnplasser fra foregående år
d_plass_fjor = (
    skjemadata["HELSE45O"][["FORETAKETS_ORGNR", "D_PLAS_T"]]
    .reset_index()
    .copy()[["FORETAKETS_ORGNR", "D_PLAS_T"]]
    .rename(columns={"D_PLAS_T": "D_PLAS_FJOR"})
)

skj45O = HF.query('ORGNR_FORETAK != "883971752"').copy()                    # Tar ut SUNNAAS

# +
# Lager kolonneoverskrifter i tråd med tidligere .csv-filer:
navn_virk, orgnr_virk = hjfunk.lag_navn_orgnr_kolonnenavn(20)

onskede_kolonner = ["USERID", "HELSEREGION", "HELSEREGION_NAVN",
                    "FORETAKETS_ORGNR", "FORETAKETS_NAVN",
                    "D_PLAS_FJOR"] + orgnr_virk + navn_virk
# -

# Henter data fra SFU med næringskode ("86.105") og statuskode ("B"). Finner næringskoden i kolonne ```SN07_1```

skj45O['tmp_bool'] = True

finne_virksomheter_df = pd.merge(
    SFUklass, skj45O, how="left", on=["ORGNR_FORETAK", "NAVN_KLASS", "HELSEREGION"]
)
finne_virksomheter_df = finne_virksomheter_df.query(
    'tmp_bool == True and SN07_1 == "86.105" and STATUS == "B"'
)
finne_virksomheter_df = finne_virksomheter_df[["ORGNR", "ORGNR_FORETAK", "NAVN"]]

undervirksomheter_navn_og_kolonner = hjfunk.lag_navn_orgnr_kolonner(finne_virksomheter_df, 20)

skj45O = pd.merge(skj45O, undervirksomheter_navn_og_kolonner, how="left", on="ORGNR_FORETAK")

skj45O['USERID'] = skj45O['ORGNR_FORETAK']
skj45O = skj45O.rename(columns={"ORGNR_FORETAK": "FORETAKETS_ORGNR",
                                "NAVN_KLASS": "FORETAKETS_NAVN"})

skj45O = pd.merge(skj45O, d_plass_fjor, how="left", on="FORETAKETS_ORGNR")

skj45O = pd.merge(skj45O, regionoppslag, how="left", on="HELSEREGION")

skj45O = skj45O[onskede_kolonner]

# ### skj46O (Somatiske sykehus, offentlige helseforetak)
# ```SUNNAAS``` skal være med her
#
# NB. Antall avsatte kolonner til undervirksomheter er 24.

# Henter døgnplasser fra foregående år
d_plass_fjor = (
    skjemadata["46O"][["ORGNR_VIRK1", "SEN_HT", "SDGN_HT"]]
    .reset_index()
    .copy()[["ORGNR_VIRK1", "SEN_HT", "SDGN_HT"]]
    .rename(
        columns={
            "SEN_HT": "SEN_HT_FJOR",
            "SDGN_HT": "SDGN_HT_FJOR",
            "ORGNR_VIRK1": "FINST_ORGNR",
        }
    )
)

skj46O = HF.copy()

# +
# Lager kolonneoverskrifter i tråd med tidligere .csv-filer:
navn_virk, orgnr_virk = hjfunk.lag_navn_orgnr_kolonnenavn(24)

kolonner = (
    [
        "USERID",
        "HELSEREGION",
        "HELSEREGION_NAVN",
        "FORETAKETS_ORGNR",
        "FORETAKETS_NAVN",
        "SEN_HT_FJOR",
        "SDGN_HT_FJOR",
    ]
    + orgnr_virk
    + navn_virk
)
# -

# Henter data fra SFU med næringskoder ("86.101") ("86.102") ("86.103") ("86.107"). Finner næringskoden i kolonne ```SN07_1```





# +
# Oppdater denne i tråd med skj38O (Løst?)
# +
skj46O["tmp_bool"] = True

finne_virksomheter_df = pd.merge(
    SFUklass, skj46O, how="left", on=["ORGNR_FORETAK", "NAVN_KLASS", "HELSEREGION"]
)
finne_virksomheter_df = finne_virksomheter_df[
    (finne_virksomheter_df['tmp_bool']) &
    (finne_virksomheter_df['SN07_1'].isin(["86.101", "86.102", "86.103", "86.107"]))
]

finne_virksomheter_df = finne_virksomheter_df[
    ["ORGNR", "ORGNR_FORETAK", "NAVN", "NAVN_KLASS"]
]
# -


finne_virksomheter_df = pd.merge(
    finne_virksomheter_df,
    d_plass_fjor,
    how="left",
    left_on="ORGNR",
    right_on="FINST_ORGNR",
)


finne_virksomheter_df[["SEN_HT_FJOR", "SDGN_HT_FJOR"]] = finne_virksomheter_df[
    ["SEN_HT_FJOR", "SDGN_HT_FJOR"]
].astype("Int64")


finne_virksomheter_df = (
    finne_virksomheter_df.groupby(["ORGNR_FORETAK", "NAVN_KLASS"])
    .sum(numeric_only=True)
    .reset_index()
)
finne_virksomheter_df = finne_virksomheter_df.drop(columns=["NAVN_KLASS"])


finne_virksomheter_df2 = pd.merge(
    SFUklass, skj46O, how="left", on=["ORGNR_FORETAK", "NAVN_KLASS", "HELSEREGION"]
)

finne_virksomheter_df2 = finne_virksomheter_df2.query(
    'tmp_bool == True and SN07_1 in ["86.101", "86.102", "86.103","86.107",]'
)

finne_virksomheter_df2 = finne_virksomheter_df2[
    ["ORGNR", "ORGNR_FORETAK", "NAVN", "NAVN_KLASS"]
]



undervirksomheter_navn_og_kolonner = hjfunk.lag_navn_orgnr_kolonner(
    finne_virksomheter_df2, 26, False
)

# +
skj46O = pd.merge(
    skj46O, undervirksomheter_navn_og_kolonner, how="left", on="ORGNR_FORETAK"
)

skj46O = pd.merge(skj46O, finne_virksomheter_df, how="left", on="ORGNR_FORETAK")

skj46O["USERID"] = skj46O["ORGNR_FORETAK"]
skj46O = skj46O.rename(
    columns={"ORGNR_FORETAK": "FORETAKETS_ORGNR", "NAVN_KLASS": "FORETAKETS_NAVN"}
)

# +
skj46O = pd.merge(skj46O, regionoppslag, how="left", on="HELSEREGION")

skj46O = skj46O[kolonner]
# -

# ## Skjemaer til private foretak og deres underinstitusjoner
# Droplisten inneholder kolonne med rapporteringspliktige underinstitusjoner i en kolonne atskilt med \n

kolonner_i_alle_private = [
    "USERID",
    "REGION_NR",
    "REGION_NAVN",
    "FORETAK_ORGNR",
    "FORETAK_NAVN",
    "FINST_ORGNR",
    "FINST_NAVN",
]

# ### skj38P (TSB for private helseforetak)

# Henter inn alle rader i SFU som har skjematype 381. Variabelen ```H_VAR1_A```  sier hvilket orgnummer foretaket rapporterer til. Koden under bruker denne til å variabelen til å plassere rapporteringsenhetene først, og alle deres underinstitusjoner i variabelen ```INSTLIST```. 
#
# For at filen skal ha riktig format, skal det tilsammen være 13 institusjoner per foretak i ```INSTLIST```. Fyller automatisk ut linjeskift ```\n``` slik at antallet blir 13.

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private + ["INSTLIST"]

skj38P = hjfunk.tabell_som_inneholder_skjema(SFUklass, "SKJEMA_TYPE", "381").copy()
skj38P = skj38P.rename(
    columns={
        "ORGNR_FORETAK": "FORETAK_ORGNR",
        "NAVN": "FINST_NAVN",
        "ORGNR": "FINST_ORGNR",
        "HELSEREGION": "REGION_NR",
        "HELSEREGION_NAVN": "REGION_NAVN",
    }
)

# USERID er alltid foretaksnummer
skj38P["USERID"] = skj38P["FORETAK_ORGNR"]
# -


# Importerer riktig regionnummer fra KLASS og gir foretak som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj38P = pd.merge(
    skj38P, regionoppslag, how="left", left_on="REGION_NR", right_on="HELSEREGION"
)

skj38P["REGION_NAVN"] = skj38P["HELSEREGION_NAVN"]
skj38P["REGION_NAVN"] = skj38P["REGION_NAVN"].fillna("PRIVATE INSTITUSJONER")

foretaksnavn = hjfunk.hent_foretaksnavn_til_virksomhetene_fra_SFU(
    skj38P.FORETAK_ORGNR.unique(), SFUklass
)
skj38P = pd.merge(skj38P, foretaksnavn, how="left", on="FORETAK_ORGNR")

rapporteringsenhet, undervirksomheter = hjfunk.instlist_med_riktig_antall_n(
    pd.DataFrame(skj38P["FINST_ORGNR"]), SFUklass
)

# +
skj38P = pd.merge(skj38P, rapporteringsenhet, how="left", on="FINST_ORGNR")
skj38P = pd.merge(skj38P, undervirksomheter, how="left", on="FINST_ORGNR")

skj38P = hjfunk.legg_paa_hale_med_n(skj38P)
# -

skj38P = skj38P[kolonner]

# ### skj39 (Resultatregnskap for private helseforetak)

# Lages på nesten samme måte som skjema 38P, med unntak av at man henter ut SKJEMA_TYPE 39 fra SFU og legger til kolonnen ```INSTTYPE``` med institusjonstype hentet fra hjelpevariabelen ```SN07_1```.

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private + ["INSTTYPE", "INSTLIST"]

skj39 = hjfunk.tabell_som_inneholder_skjema(SFUklass, "SKJEMA_TYPE", "39").copy()
skj39 = skj39.rename(
    columns={
        "ORGNR_FORETAK": "FORETAK_ORGNR",
        "NAVN": "FINST_NAVN",
        "ORGNR": "FINST_ORGNR",
        "HELSEREGION": "REGION_NR",
        "HELSEREGION_NAVN": "REGION_NAVN",
    }
)

# USERID er alltid foretaksnummer
skj39["USERID"] = skj39["FORETAK_ORGNR"]
# -


# Importerer riktig regionnummer fra KLASS og gir foretak som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj39 = pd.merge(
    skj39, regionoppslag, how="left", left_on="REGION_NR", right_on="HELSEREGION"
)

skj39["REGION_NAVN"] = skj39["HELSEREGION_NAVN"]
skj39["REGION_NAVN"] = skj39["REGION_NAVN"].fillna("PRIVATE INSTITUSJONER")

# +
# Henter foretaksnavn til virksomhetene fra SFU
foretak = hjfunk.hent_foretaksnavn_til_virksomhetene_fra_SFU(
    skj39.FORETAK_ORGNR.unique(), SFUklass
)

skj39 = pd.merge(skj39, foretak, how="left", on="FORETAK_ORGNR")
# -

rapporteringsenhet, undervirksomheter = hjfunk.instlist_med_riktig_antall_n(
    pd.DataFrame(skj39["FINST_ORGNR"]), SFUklass
)

# +
# skj39 = pd.merge(skj39, rapporteringsenhet, how="left", on="FINST_ORGNR")
# skj39 = pd.merge(skj39, undervirksomheter, how="left", on="FINST_ORGNR")
# skj39 = hjfunk.legg_paa_hale_med_n(skj39)

# +
institusjonstype = {
    451: "Psykisk helsevern for barn og unge",
    461: "Somatiske sykehus",
    381: "Rusmiddelinstitusjoner",
    441: "Psykisk helsevern for voksne",
    47: "Somatiske rehab.-og opptr.inst.",
}

skj39 = skj39.rename(columns={"H_VAR2_N": "INSTTYPE"})
# -

skj39['INSTTYPE'] = skj39['INSTTYPE'].map(institusjonstype)

# Hvis FORETAK_NAVN er tom, bruk NAVN1 fra SFU:
skj39.loc[skj39['FORETAK_NAVN'].isnull(), 'FORETAK_NAVN'] = skj39['NAVN1']

# +
# Tar kun vare på de kolonnene jeg spesifiserer i begynnelsen
# skj39 = skj39[kolonner]
# -

# ### skj39b (Resultatregnskap for private helseforetak) nytt format

skj39b = hjfunk.tabell_som_inneholder_skjema(SFUklass, 'SKJEMA_TYPE', "39").copy()

skjemadata39 = skjemadata["HELSE39"].reset_index()

ifjor_kol = ["ART_30", "ART_31", "ART_32", "A32_DRG",
             "A32_DRG_KOM", "A32_GJPAS", "A32_EGEN", "A32_SELV",
             "A32_UTKLAR", "A32_ANDRE", "ART_33", "ART_34",
             "A34_POLI", "A34_LAB", "A34_DRIFT", "A34_FBV",
             "A34_RT", "A34_FORSK", "A34_UNDER", "A34_FUNK",
             "A34_HREG", "A34_KREFT", "A34_PSYK", "A34_ATILSK1",
             "A34_ATILSK2", "A34_ATILSK3", "A34_ATILSK4", "A34_ATILSK5",
             "ART_35", "A35_GAVE", "ART_36", "ART_37",
             "ART_38", "ART_39", "SALG_SUM", "ART_40",
             "A40_MEDIK", "A40_BLOD", "A40_IMPLA", "A40_INSTR",
             "A40_LABREK", "A40_RONTG", "A40_INFUS", "A40_AFORB",
             "ART_41", "ART_42", "ART_43", "ART_45",
             "A45_OFFH", "A45_PRIV", "A45_DIV", "ART_49",
             "VARE_SUM", "ART_50", "ART_51", "ART_52",
             "ART_53", "ART_54", "ART_54_AGA", "ART_54_PK",
             "ART_55", "ART_56", "ART_57", "ART_58",
             "ART_59", "LONN_SUM", "ART_60", "A60_DBYG",
             "A60_ABYG", "A60_TRANS", "A60_MEDTEK", "A60_ITK",
             "A60_IMAT", "A60_VARIG", "ART_61", "A61_PASTR",
             "A61_ATRANS", "ART_62", "ART_63", "ART_64",
             "ART_65", "ART_66", "ART_67", "ART_68",
             "ART_69", "ART_70", "ART_71", "ART_72",
             "ART_73", "ART_74", "ART_75", "ART_76",
             "ART_77", "ART_78", "ART_79", "DRIFT_SUM",
             "ART_80", "ART_81", "ART_83", "ART_84",
             "ART_85", "ART_86", "ART_88", "ART_89",
             "FINNT_SUM", "TOTOVF_HF", "OVF_DRTILSK_SUM", "OVF_AKTBAS_SUM",
             "OVF_GJPINT_SUM", "OVF_ATILSK_SUM", "OVF_SUM_SUM",
             ]

skj39b = pd.merge(
    skj39,
    skjemadata39[["FINST_ORGNR"] + ifjor_kol],
    on="FINST_ORGNR",
    how="left"
)

ifjor_kol_med_suffix = [kol + "_IFJOR" for kol in ifjor_kol]

nye_kolonnenavn = dict(zip(ifjor_kol, ifjor_kol_med_suffix))

skj39b = skj39b.rename(columns=nye_kolonnenavn)

# ### skj44P (Psykisk helsevern for voksne (PHFV), private helseforetak)

# Henter døgnplasser fra foregående år
d_plass_fjor = (skjemadata["HELSE44P"][['FINST_ORGNR','D_PLAS_T']].reset_index().copy()
                [['FINST_ORGNR','D_PLAS_T']]
                .rename(columns={'D_PLAS_T': 'D_PLAS_FJOR'}))

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private + ["INSTLIST", "D_PLAS_FJOR"]

skj44P = hjfunk.tabell_som_inneholder_skjema(SFUklass, "SKJEMA_TYPE", "441").copy()
skj44P = skj44P.rename(
    columns={
        "ORGNR_FORETAK": "FORETAK_ORGNR",
        "NAVN": "FINST_NAVN",
        "ORGNR": "FINST_ORGNR",
        "HELSEREGION": "REGION_NR",
        "HELSEREGION_NAVN": "REGION_NAVN",
    }
)

# USERID er alltid foretaksnummer
skj44P["USERID"] = skj44P["FORETAK_ORGNR"]
# -


# Importerer riktig regionnummer fra KLASS og gir foretak 
# som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj44P = pd.merge(skj44P, regionoppslag, how="left" , left_on="REGION_NR", right_on="HELSEREGION")

skj44P['REGION_NAVN'] = skj44P['HELSEREGION_NAVN']
skj44P.REGION_NAVN = skj44P.REGION_NAVN.fillna("PRIVATE INSTITUSJONER")

# +
# Henter foretaksnavn til virksomhetene fra SFU
foretak = hjfunk.hent_foretaksnavn_til_virksomhetene_fra_SFU(skj44P.FORETAK_ORGNR.unique(), SFUklass)

skj44P = pd.merge(skj44P, foretak, how="left", on="FORETAK_ORGNR")
# -

rapporteringsenhet, undervirksomheter = hjfunk.instlist_med_riktig_antall_n(pd.DataFrame(skj44P['FINST_ORGNR']), SFUklass)

skj44P = pd.merge(skj44P, rapporteringsenhet, how="left", on="FINST_ORGNR")
skj44P = pd.merge(skj44P, undervirksomheter, how="left", on="FINST_ORGNR")
skj44P = hjfunk.legg_paa_hale_med_n(skj44P)

skj44P = pd.merge(skj44P, d_plass_fjor, how="left", on="FINST_ORGNR")

# Tar kun vare på de kolonnene jeg spesifiserte i begynnelsen
skj44P = skj44P[kolonner]

# ### skj45P (Psykisk helsevern for barn og unge (PHBU/BUP), private helseforetak)

# Henter døgnplasser fra foregående år
d_plass_fjor = (skjemadata["HELSE45P"][['FINST_ORGNR','D_PLAS_T']].reset_index().copy()
                [['FINST_ORGNR','D_PLAS_T']]
                .rename(columns={'D_PLAS_T': 'D_PLAS_FJOR'}))

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private + ["INSTLIST", "D_PLAS_FJOR"]

skj45P = hjfunk.tabell_som_inneholder_skjema(SFUklass, "SKJEMA_TYPE", "451").copy()
skj45P = skj45P.rename(
    columns={
        "ORGNR_FORETAK": "FORETAK_ORGNR",
        "NAVN": "FINST_NAVN",
        "ORGNR": "FINST_ORGNR",
        "HELSEREGION": "REGION_NR",
        "HELSEREGION_NAVN": "REGION_NAVN",
    }
)

# USERID er alltid foretaksnummer
skj45P["USERID"] = skj45P["FORETAK_ORGNR"]

# +
# Importerer riktig regionnummer fra KLASS og gir foretak
# som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj45P = pd.merge(
    skj45P, regionoppslag, how="left", left_on="REGION_NR", right_on="HELSEREGION"
)

skj45P["REGION_NAVN"] = skj45P["HELSEREGION_NAVN"]
skj45P.REGION_NAVN = skj45P.REGION_NAVN.fillna("PRIVATE INSTITUSJONER")

# +
# Henter foretaksnavn til virksomhetene fra SFU
foretak = hjfunk.hent_foretaksnavn_til_virksomhetene_fra_SFU(
    skj45P.FORETAK_ORGNR.unique(), SFUklass
)

skj45P = pd.merge(skj45P, foretak, how="left", on="FORETAK_ORGNR")
# -

rapporteringsenhet, undervirksomheter = hjfunk.instlist_med_riktig_antall_n(
    pd.DataFrame(skj45P["FINST_ORGNR"]), SFUklass
)

skj45P = pd.merge(skj45P, rapporteringsenhet, how="left", on="FINST_ORGNR")

# +
n_tom = r"\n"*11

if undervirksomheter is not None:
    skj45P = pd.merge(skj45P, undervirksomheter, how="left", on="FINST_ORGNR")
    skj45P['INSTLISTE_HALE'] = skj45P['INSTLISTE_HALE'].fillna(n_tom)
else:
    skj45P['INSTLISTE_HALE'] = n_tom

skj45P = hjfunk.legg_paa_hale_med_n(skj45P)
# -

skj45P = pd.merge(skj45P, d_plass_fjor, how="left", on="FINST_ORGNR")

# Tar kun vare på de kolonnene spesifisert i begynnelsen
skj45P = skj45P[kolonner]

# ### skj46P (Somatiske sykehus, private helseforetak)

# Henter døgnplasser fra foregående år
d_plass_fjor = (
    skjemadata["HELSE46P"][["FINST_ORGNR", "SEN_HT", "SDGN_HT"]]
    .reset_index()
    .copy()[["FINST_ORGNR", "SEN_HT", "SDGN_HT"]]
    .rename(columns={"SEN_HT": "SEN_HT_FJOR", "SDGN_HT": "SDGN_HT_FJOR"})
)

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private + ["SEN_HT_FJOR", "SDGN_HT_FJOR"]

skj46P = hjfunk.tabell_som_inneholder_skjema(SFUklass, "SKJEMA_TYPE", "461").copy()
skj46P = skj46P.rename(
    columns={
        "ORGNR_FORETAK": "FORETAK_ORGNR",
        "NAVN": "FINST_NAVN",
        "ORGNR": "FINST_ORGNR",
        "HELSEREGION": "REGION_NR",
        "HELSEREGION_NAVN": "REGION_NAVN",
    }
)

# USERID er alltid foretaksnummer
skj46P["USERID"] = skj46P["FORETAK_ORGNR"]


# +
# Importerer riktig regionnummer fra KLASS og gir foretak som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj46P = pd.merge(
    skj46P, regionoppslag, how="left", left_on="REGION_NR", right_on="HELSEREGION"
)

skj46P["REGION_NAVN"] = skj46P["HELSEREGION_NAVN"]
skj46P.REGION_NAVN = skj46P.REGION_NAVN.fillna("PRIVATE INSTITUSJONER")

# +
# Henter foretaksnavn til virksomhetene fra SFU
foretak = hjfunk.hent_foretaksnavn_til_virksomhetene_fra_SFU(
    skj46P.FORETAK_ORGNR.unique(), SFUklass
)

skj46P = pd.merge(skj46P, foretak, how="left", on="FORETAK_ORGNR")
# -

rapporteringsenhet, undervirksomheter = hjfunk.instlist_med_riktig_antall_n(
    pd.DataFrame(skj46P["FINST_ORGNR"]), SFUklass
)

skj46P = pd.merge(skj46P, rapporteringsenhet, how="left", on="FINST_ORGNR")

# +
skj46P = pd.merge(skj46P, undervirksomheter, how="left", on="FINST_ORGNR")
skj46P.orgnr_navn = skj46P.orgnr_navn.fillna("")

skj46P = hjfunk.legg_paa_hale_med_n(skj46P)
# -

skj46P = pd.merge(skj46P, d_plass_fjor, how="left", on="FINST_ORGNR")

skj46P.loc[skj46P.FORETAK_NAVN.isnull(), 'FORETAK_NAVN'] = skj46P.NAVN1

# Tar kun vare på de kolonnene jeg spesifiserte i begynnelsen
skj46P = skj46P[kolonner]

# ### skj46Pb (Somatiske sykehus, private helseforetak) Alternativ dropliste

# Legger ved en kolonne `INSTLIST` som for skj39.

skj46Pb = skj46P.copy()

rapporteringsenhet, undervirksomheter = hjfunk.instlist_med_riktig_antall_n(
    pd.DataFrame(skj46Pb["FINST_ORGNR"]), SFUklass
)

skj46Pb = pd.merge(skj46Pb, rapporteringsenhet, how="left", on="FINST_ORGNR")
skj46Pb = pd.merge(skj46Pb, undervirksomheter, how="left", on="FINST_ORGNR")
skj46Pb = hjfunk.legg_paa_hale_med_n(skj46Pb)

skj46Pb = skj46Pb[kolonner + ['INSTLIST']]

# ### skj47 (Somatiske institusjoner)

# Henter døgnplasser fra foregående år
d_plass_fjor = (
    skjemadata["HELSE47"][["FINST_ORGNR", "D_HELD", "SDGN_SUM"]]
    .reset_index()
    .copy()[["FINST_ORGNR", "D_HELD", "SDGN_SUM"]]
    .rename(columns={"D_HELD": "D_HELD_IFJOR", "SDGN_SUM": "SDGN_HT_FJOR"})
)

# +
# kolonner som skal være med i droplisten:
kolonner = kolonner_i_alle_private + ["D_HELD_IFJOR", "SDGN_HT_FJOR"]

skj47 = hjfunk.tabell_som_inneholder_skjema(SFUklass, "SKJEMA_TYPE", "47").copy()
skj47 = skj47.rename(
    columns={
        "ORGNR_FORETAK": "FORETAK_ORGNR",
        "NAVN": "FINST_NAVN",
        "ORGNR": "FINST_ORGNR",
        "HELSEREGION": "REGION_NR",
        "HELSEREGION_NAVN": "REGION_NAVN",
    }
)

# USERID er alltid foretaksnummer
skj47["USERID"] = skj47["FORETAK_ORGNR"]

# +
# Importerer riktig regionnummer fra KLASS og gir foretak som ikke er offentlige betegnelsen "PRIVATE INSTITUSJONER"
skj47 = pd.merge(
    skj47, regionoppslag, how="left", left_on="REGION_NR", right_on="HELSEREGION"
)

skj47["REGION_NAVN"] = skj47["HELSEREGION_NAVN"]
skj47.REGION_NAVN = skj47.REGION_NAVN.fillna("PRIVATE INSTITUSJONER")
# -



# Henter foretaksnavn til virksomhetene fra SFU
foretak = hjfunk.hent_foretaksnavn_til_virksomhetene_fra_SFU(
    skj47.FORETAK_ORGNR.unique(), SFUklass
)
skj47 = pd.merge(skj47, foretak, how="left", on="FORETAK_ORGNR")

rapporteringsenhet, undervirksomheter = hjfunk.instlist_med_riktig_antall_n(
    pd.DataFrame(skj47["FINST_ORGNR"]), SFUklass
)

skj47 = pd.merge(skj47, rapporteringsenhet, how="left", on="FINST_ORGNR")
skj47 = pd.merge(skj47, undervirksomheter, how="left", on="FINST_ORGNR")
skj47 = hjfunk.legg_paa_hale_med_n(skj47)

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
    eval(skj).to_csv(sti_til_lagring + filnavn, sep=";", encoding="latin1", index=False)
    print(sti_til_lagring + filnavn, " lagret")


# -

for x in skjemaer_til_droplister:
    print(rapport(x))

if lagre_filer:
    for x in skjemaer_til_droplister:
        lagre_dropliste_csv(x)

# Lukk tilkoblingen
conn.close()


