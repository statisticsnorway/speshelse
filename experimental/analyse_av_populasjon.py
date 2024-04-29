# -*- coding: utf-8 -*-
# # Prep

# ## Pakker og tilganger

import sys
sys.path.insert(0, '..')

import Droplister.hjelpefunksjoner as hjfunk
from functions.hjelpefunksjoner import lagre_excel

# +
import pandas as pd
import numpy as np

import getpass
import datetime as dt
import requests

from sqlalchemy import create_engine
import getpass
# -

""
til_lagring = True # Sett til True, hvis du vil lagre en ny fil
""
import functools as ft

pd.options.display.float_format = '{:.1f}'.format

username = getpass.getuser()
dsn = "DB1P"
try:
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")
except:
    password = getpass.getpass(prompt='Oracle-passord: ')
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")

# Opprett en tilkobling fra motoren
conn = engine.connect()


aar4 = '2023'
aar2 = aar4[-2:]


def rapport_df(df):
    display(df.info())
    if len(df) > 0:
        display(df.sample(1))


# # Innlasting av filer

sti_akt   = f"/ssb/stamme01/fylkhels/speshelse/aktivitet/{aar4}/masterfil/aktivitet_masterfil_{aar4}.parquet"
sti_rgn0x = f"/ssb/stamme01/fylkhels/speshelse/regnskap/{aar4}/masterfil/helse0x_masterfil_{aar4}.parquet"
sti_rgn39 = f"/ssb/stamme01/fylkhels/speshelse/regnskap/{aar4}/masterfil/helse39_masterfil_{aar4}.parquet"
sti_per   = f"/ssb/stamme01/fylkhels/speshelse/personell/{aar4}/personell/masterfil/personell_masterfil_{aar4}.parquet"

try:
    akt = pd.read_parquet(sti_akt)
except FileNotFoundError:
    sti_akt_2022 = f"/ssb/stamme01/fylkhels/speshelse/aktivitet/2022/masterfil/aktivitet_masterfil_2022.parquet"
    akt = pd.read_parquet(sti_akt_2022)
    akt = pd.DataFrame(columns=akt.columns)
    print(f"Aktivitetsfil ikke funnet: {sti_akt}. Opprettet en tom dataframe basert på kolonnene i masterfilen i 2022.")

rgn0x = pd.read_parquet(sti_rgn0x)

rgn39 = pd.read_parquet(sti_rgn39)

per = pd.read_parquet(sti_per)

forventede_kolonner = {
    "akt": [
        "ORGNR_FRTK",
        "NAVN_FRTK",
        "TJENESTE_KODE",
        "UTSKRIVNINGER",
        "OPPHOLDSDOGN",
        "OPPHOLDSDAGER",
        "POLIKLINIKK",
        "DOGNPLASSER",
        "SENGEDOGN",
        "RHF",
        "HELSEREGION",
        "FORETAKSTYPE",
        "ORGNR_VIRK",
        "NAVN_VIRK",
        "TJENESTE_NAVN",
        "ORGNR_STATBANK",
        "NAVN_STATBANK",
    ],
    "rgn0x": [
        "DELREG_NR",
        "SKJEMA",
        "LOPENR",
        "RAD_NR",
        "LINJENUMMER",
        "ART_SEKTOR",
        "AVGIVER_ID",
        "BYDEL",
        "FORETAKSNR",
        "FUNKSJON_KAPITTEL",
        "KONTOKLASSE",
        "KVARTAL",
        "SSB_LOGDATO",
        "TELEFONNR",
        "VERDATO",
        "IDENT_NR",
        "ORGNR_DELREG",
        "NACE1_DELREG",
        "NACE2_DELREG",
        "SN07_1_DELREG",
        "SN07_2_DELREG",
        "ORG_FORM",
        "ORGNR_FRTK",
        "NOKKEL",
        "BELOP",
        "ENHETS_ID",
        "ENHETS_TYPE",
        "NAVN",
        "AARGANG",
        "NAVN_DELREG",
        "REGION",
        "NAVN_FRTK",
        "RHF",
        "HELSEREGION",
        "FORETAKSTYPE",
        "tid",
        "FUNKSJON_KAPITTEL_NAVN",
        "TJENESTE_KODE",
        "TJENESTE_NAVN",
        "TOT_UTG",
        "F321",
        "F327",
        "TOT_UTG_LANDET",
        "MEDISIN",
        "KJOPOFFTJEN",
        "KJOPPRIVTJEN",
        "KJOPBEHUTLAND",
        "LONN",
        "PENSJON_KOST",
        "AVSKRIVNING",
        "SYKETRANS",
        "MEDTEKUTS",
        "ANDRKOST",
        "BDI",
        "BDI_LANDET",
        "BDI_REG",
        "DRGINNT",
        "GJESTEPASINNT",
        "RTVINNT",
        "EGENANDELER",
        "TILSKUDDREFUSJON",
        "ANDINNT",
        "FINANSINNTEKTER",
        "FINANSKOSTNADER",
        "AARSRESULTAT",
        "SKATT",
        "TOTOVF_HF",
        "BDU",
        "KJOPVARTJEN",
        "ORGNR_STATBANK",
        "NAVN_STATBANK",
    ],
    "rgn39": [
        "SKJEMA",
        "ENHETS_ID",
        "ENHETS_TYPE",
        "DELREG_NR",
        "LOPENR",
        "RAD_NR",
        "A34_NAVN1",
        "A34_NAVN2",
        "A34_NAVN3",
        "A34_NAVN4",
        "A34_NAVN5",
        "AARGANG",
        "AVGIVER_ID",
        "FINST_INKL",
        "ORGNR_VIRK",
        "FORETAKETS_NAVN",
        "HELSEREGION_EPOST",
        "HELSEREGION_NAVN",
        "HELSEREGION_TLF",
        "HELSEREGION_UTFYLT",
        "INSTITUSJONSTYPE",
        "MERKNAD1",
        "OVF_HFRHF_1",
        "OVF_HFRHF_10",
        "OVF_HFRHF_11",
        "OVF_HFRHF_12",
        "OVF_HFRHF_13",
        "OVF_HFRHF_14",
        "OVF_HFRHF_15",
        "OVF_HFRHF_16",
        "OVF_HFRHF_17",
        "OVF_HFRHF_18",
        "OVF_HFRHF_19",
        "OVF_HFRHF_2",
        "OVF_HFRHF_20",
        "OVF_HFRHF_3",
        "OVF_HFRHF_4",
        "OVF_HFRHF_5",
        "OVF_HFRHF_6",
        "OVF_HFRHF_7",
        "OVF_HFRHF_8",
        "OVF_HFRHF_9",
        "RESPONS1",
        "SSB_LOGDATO",
        "VERDATO",
        "A32_ANDRE",
        "A32_DRG",
        "A32_DRG_KOM",
        "A32_EGEN",
        "A32_GJPAS",
        "A32_SELV",
        "A32_UTKLAR",
        "A34_ATILSK1",
        "A34_ATILSK2",
        "A34_ATILSK3",
        "A34_ATILSK4",
        "A34_ATILSK5",
        "A34_DRIFT",
        "A34_FBV",
        "A34_FORSK",
        "A34_FUNK",
        "A34_HREG",
        "A34_KREFT",
        "A34_LAB",
        "A34_POLI",
        "A34_PSYK",
        "A34_RT",
        "A34_UNDER",
        "A35_GAVE",
        "A40_AFORB",
        "A40_BLOD",
        "A40_IMPLA",
        "A40_INFUS",
        "A40_INSTR",
        "A40_LABREK",
        "A40_MEDIK",
        "A40_RONTG",
        "A45_DIV",
        "A45_OFFH",
        "A45_PRIV",
        "A60_ABYG",
        "A60_DBYG",
        "A60_IMAT",
        "A60_ITK",
        "A60_MEDTEK",
        "A60_TRANS",
        "A60_VARIG",
        "A61_ATRANS",
        "A61_PASTR",
        "ART_30",
        "ART_31",
        "ART_32",
        "ART_33",
        "ART_34",
        "ART_35",
        "ART_36",
        "ART_37",
        "ART_38",
        "ART_39",
        "ART_40",
        "ART_41",
        "ART_42",
        "ART_43",
        "ART_45",
        "ART_49",
        "ART_50",
        "ART_51",
        "ART_52",
        "ART_53",
        "ART_54",
        "ART_54_AGA",
        "ART_54_PK",
        "ART_55",
        "ART_56",
        "ART_57",
        "ART_58",
        "ART_59",
        "ART_60",
        "ART_61",
        "ART_62",
        "ART_63",
        "ART_64",
        "ART_65",
        "ART_66",
        "ART_67",
        "ART_68",
        "ART_69",
        "ART_70",
        "ART_71",
        "ART_72",
        "ART_73",
        "ART_74",
        "ART_75",
        "ART_76",
        "ART_77",
        "ART_78",
        "ART_79",
        "ART_80",
        "ART_81",
        "ART_83",
        "ART_84",
        "ART_85",
        "ART_86",
        "ART_88",
        "ART_89",
        "DRIFT_SUM",
        "DRIFTKOSTNAD_SUM",
        "DSUM",
        "F32",
        "F34",
        "FINNT_SUM",
        "FSUM",
        "HJELP_1",
        "HJELP_2",
        "HJELP_3",
        "HJELP_5",
        "HJELPE_2",
        "IAAR",
        "IFJOR",
        "LONN_SUM",
        "LSUM",
        "OVF_AKTBAS_1",
        "OVF_AKTBAS_10",
        "OVF_AKTBAS_11",
        "OVF_AKTBAS_12",
        "OVF_AKTBAS_13",
        "OVF_AKTBAS_14",
        "OVF_AKTBAS_15",
        "OVF_AKTBAS_16",
        "OVF_AKTBAS_17",
        "OVF_AKTBAS_18",
        "OVF_AKTBAS_19",
        "OVF_AKTBAS_2",
        "OVF_AKTBAS_20",
        "OVF_AKTBAS_3",
        "OVF_AKTBAS_4",
        "OVF_AKTBAS_5",
        "OVF_AKTBAS_6",
        "OVF_AKTBAS_7",
        "OVF_AKTBAS_8",
        "OVF_AKTBAS_9",
        "OVF_AKTBAS_SUM",
        "OVF_ATILSK_1",
        "OVF_ATILSK_10",
        "OVF_ATILSK_11",
        "OVF_ATILSK_12",
        "OVF_ATILSK_13",
        "OVF_ATILSK_14",
        "OVF_ATILSK_15",
        "OVF_ATILSK_16",
        "OVF_ATILSK_17",
        "OVF_ATILSK_18",
        "OVF_ATILSK_19",
        "OVF_ATILSK_2",
        "OVF_ATILSK_20",
        "OVF_ATILSK_3",
        "OVF_ATILSK_4",
        "OVF_ATILSK_5",
        "OVF_ATILSK_6",
        "OVF_ATILSK_7",
        "OVF_ATILSK_8",
        "OVF_ATILSK_9",
        "OVF_ATILSK_SUM",
        "OVF_DRTILSK_1",
        "OVF_DRTILSK_10",
        "OVF_DRTILSK_11",
        "OVF_DRTILSK_12",
        "OVF_DRTILSK_13",
        "OVF_DRTILSK_14",
        "OVF_DRTILSK_15",
        "OVF_DRTILSK_16",
        "OVF_DRTILSK_17",
        "OVF_DRTILSK_18",
        "OVF_DRTILSK_19",
        "OVF_DRTILSK_2",
        "OVF_DRTILSK_20",
        "OVF_DRTILSK_3",
        "OVF_DRTILSK_4",
        "OVF_DRTILSK_5",
        "OVF_DRTILSK_6",
        "OVF_DRTILSK_7",
        "OVF_DRTILSK_8",
        "OVF_DRTILSK_9",
        "OVF_DRTILSK_SUM",
        "OVF_GJPINT_1",
        "OVF_GJPINT_10",
        "OVF_GJPINT_11",
        "OVF_GJPINT_12",
        "OVF_GJPINT_13",
        "OVF_GJPINT_14",
        "OVF_GJPINT_15",
        "OVF_GJPINT_16",
        "OVF_GJPINT_17",
        "OVF_GJPINT_18",
        "OVF_GJPINT_19",
        "OVF_GJPINT_2",
        "OVF_GJPINT_20",
        "OVF_GJPINT_3",
        "OVF_GJPINT_4",
        "OVF_GJPINT_5",
        "OVF_GJPINT_6",
        "OVF_GJPINT_7",
        "OVF_GJPINT_8",
        "OVF_GJPINT_9",
        "OVF_GJPINT_SUM",
        "OVF_REGION_1",
        "OVF_REGION_10",
        "OVF_REGION_11",
        "OVF_REGION_12",
        "OVF_REGION_13",
        "OVF_REGION_14",
        "OVF_REGION_15",
        "OVF_REGION_16",
        "OVF_REGION_17",
        "OVF_REGION_18",
        "OVF_REGION_19",
        "OVF_REGION_2",
        "OVF_REGION_20",
        "OVF_REGION_3",
        "OVF_REGION_4",
        "OVF_REGION_5",
        "OVF_REGION_6",
        "OVF_REGION_7",
        "OVF_REGION_8",
        "OVF_REGION_9",
        "OVF_SUM_1",
        "OVF_SUM_10",
        "OVF_SUM_11",
        "OVF_SUM_12",
        "OVF_SUM_13",
        "OVF_SUM_14",
        "OVF_SUM_15",
        "OVF_SUM_16",
        "OVF_SUM_17",
        "OVF_SUM_18",
        "OVF_SUM_19",
        "OVF_SUM_2",
        "OVF_SUM_20",
        "OVF_SUM_3",
        "OVF_SUM_4",
        "OVF_SUM_5",
        "OVF_SUM_6",
        "OVF_SUM_7",
        "OVF_SUM_8",
        "OVF_SUM_9",
        "OVF_SUM_SUM",
        "SALG_SUM",
        "SSUM",
        "TOTOVF_HF",
        "VARE_SUM",
        "VSUM",
        "H_VAR3_A",
        "H_VAR1_N",
        "SN07_1_DELREG",
        "tid",
        "SN07_1",
        "TJENESTE_KODE",
        "TJENESTE_NAVN",
        "TJENESTE_KODE_OBS",
        "TOT_UTG",
        "TOT_UTG_LANDET",
        "MEDISIN",
        "KJOPOFFTJEN",
        "KJOPPRIVTJEN",
        "LONN_1",
        "LONN_2",
        "LONN",
        "PENSJON_KOST",
        "AVSKRIVNING",
        "MEDTEKUTS",
        "ANDRKOST_1",
        "ANDRKOST_2",
        "ANDRKOST",
        "BDI",
        "BDI_LANDET",
        "BDI_REG",
        "DRGINNT",
        "GJESTEPASINNT",
        "RTVINNT",
        "EGENANDELER",
        "TILSKUDDREFUSJON_1",
        "TILSKUDDREFUSJON",
        "ANDINNT",
        "AARSRESULTAT",
        "FINANSINNTEKTER",
        "FINANSKOSTNADER",
        "SKATT",
        "F321",
        "F327",
        "KJOPBEHUTLAND",
        "SYKETRANS",
        "DK_EP",
        "KJOPVARTJEN",
        "BDU_UTLAND",
        "ORGNR_FRTK",
        "NAVN_FRTK",
        "RHF",
        "HELSEREGION",
        "FORETAKSTYPE",
        "NAVN_VIRK",
        "ORGNR_STATBANK",
        "NAVN_STATBANK",
    ],
    "per": [
        "ORGNR_FRTK",
        "FORETAKSTYPE",
        "HELSEREGION",
        "AARSVERK",
        "ARB_ANSETTELSESFORM",
        "ARB_ARBEIDSTID",
        "ARB_ARBEIDSTID_IMP_KVAL",
        "ARB_ARBMARK_STATUS",
        "ARB_HELDELTID",
        "ARB_HOVEDARBEID",
        "ARB_STILLINGSPST",
        "ARB_STILLINGSPST_INNRAPP",
        "ARB_STILLINGSPST_KILDE",
        "ARB_TID_ORDNING",
        "ARB_TIMEANT_FULLTID",
        "ARB_TYPE",
        "ARB_YRKE",
        "NAVN_FRTK",
        "FRTK_ORG_FORM",
        "FRTK_SEKTOR_2014",
        "FRTK_UNDERSEKTOR_2014",
        "PERS_ALDER",
        "PERS_BU_NUS2000",
        "PERS_FL_AVTALE",
        "PERS_IGANG_NUS2000",
        "PERS_INNGRUNN1",
        "PERS_INVKAT",
        "PERS_KJOENN",
        "PERS_PERSONTYPE",
        "PERS_STATSBORGERSKAP",
        "PERS_SUM_ARBEIDSTID",
        "PERS_SUM_STILLINGSPST",
        "ORGNR_VIRK",
        "VIRK_NACE1_SN07",
        "NAVN_VIRK",
        "PERS_HP_HPR_K",
        "PERS_HP_AUT_KD",
        "PERS_HP_SPES1",
        "PERS_HP_SPES2",
        "PERS_HP_SPES3",
        "VIRK_HO_MERKE",
        "PERS_LU_LARERU",
        "PERS_LU_NUS2000LU",
        "HPR_KOSTRA",
        "F_K",
        "K_YRK",
        "AKOMM_FORETAK",
        "F_KAGGR",
        "POP",
        "IKS",
        "PERS_FOEDPERM_ANDEL",
        "PERS_SYKEFRAV_ANDEL",
        "PERS_LANGEFRA_ANDEL",
        "ARB_ARBKOMM",
        "PERS_HP_AUT_DT_F",
        "PERS_HP_NASJ_KD_NY",
        "PERS_HP_NUS2000",
        "PERS_ID_SSB",
        "PERS_KOMMNR",
        "PERS_LANDBAK3GEN",
        "SYSSELSATT",
        "SYSSELSATT_HOVEDARBEID",
        "ARSVERK_EKSKL",
        "YRKESAKTIV",
        "ID_test",
        "NAVN_VIRK_LANG",
        "ARB_FYLKE_KODE",
        "HELSETJENESTE",
        "LANDBAKGRUNN",
        "LANDBAKGRUNN_2",
        "ARB_FYLKE_NAVN",
        "VIRK_NACE1_SN07_NAVN",
        "PERS_HP_NUS2000_NAVN",
        "TJENESTE_KODE",
        "TJENESTE_NAVN",
        "ARB_YRKE_NAVN",
        "STYRK_08_4_SIFFER",
        "STYRK_08_4_SIFFER_NAVN",
        "STYRK_08_1_SIFFER",
        "STYRK_08_2_SIFFER",
        "STYRK_08_3_SIFFER",
        "STYRK_08_1_SIFFER_NAVN",
        "STYRK_08_2_SIFFER_NAVN",
        "STYRK_08_3_SIFFER_NAVN",
        "STYRK08_KODE_POB",
        "STILLING_KODE",
        "STYRK08_NAVN_POB",
        "STYRK08_KODE_YRKE",
        "YRKE_KODE",
        "YRKE_NAVN",
        "PERS_HP_HPR_K_NAVN",
        "UTDANNING_KODE",
        "UTDANNING_NAVN",
        "AARGANG",
        "ORGNR_STATBANK",
        "NAVN_STATBANK",
        "PERS_HP_SPES1_NAVN",
        "PERS_HP_SPES2_NAVN",
        "PERS_HP_SPES3_NAVN",
        "PERS_HP_SPES1_NY",
        "PERS_HP_SPES1_NY_NAVN",
    ],
}

for tabell, kolonner in forventede_kolonner.items():
    kolonner_i_innlastede_tabeller_som_ikke_er_blant_de_forventede = [kol for kol in eval(tabell).columns if kol not in kolonner]
    forventede_kolonner_som_ikke_er_innlastet = [kol for kol in kolonner if kol not in eval(tabell).columns]
    print(tabell)
    print("Kolonner i innlastede tabeller som ikke er blant de forventede:\n", (", ").join(kolonner_i_innlastede_tabeller_som_ikke_er_blant_de_forventede))
    print("Forventede kolonner som ikke er innlastet:\n", (", ").join(forventede_kolonner_som_ikke_er_innlastet))
    print(70*("-"))

# # Bearbeiding av filer

# ### Foretakstype fra KLASS

HF, RHF, phob, rfss, rfss2, rfss3, rapporteringsenheter = hjfunk.hent_enheter_fra_klass(
    aar4
)

rfs = pd.concat([rfss, rfss2, rfss3], ignore_index=True)

rfs['FORETAKSTYPE'] = 'Støtteforetak'
HF['FORETAKSTYPE'] = 'HF'
phob['FORETAKSTYPE'] = 'Oppdrag'
RHF['FORETAKSTYPE'] = 'RHF'

foretakstyper = (
    pd.concat([RHF, HF, phob, rfs], ignore_index=True)
    .drop(columns=["HELSEREGION", "NAVN_KLASS"])
    .rename(columns={"ORGNR_FORETAK": "ORGNR_FRTK"})
)

# ## Aktivitet

akt.FORETAKSTYPE.value_counts()

akt.columns

akt = (
    akt
    .drop(columns=['NAVN_FRTK', 'TJENESTE_NAVN', 'RHF'])
)

as_mask = akt['FORETAKSTYPE'] == "Avtalespesialister"
akt.loc[as_mask, 'ORGNR_FRTK'] = akt.loc[as_mask, 'ORGNR_STATBANK'] 

if len(akt) > 0:
    akt = akt.groupby(['ORGNR_FRTK', 'TJENESTE_KODE', 'FORETAKSTYPE', 'HELSEREGION', 'ORGNR_STATBANK']).sum(numeric_only=True).reset_index()

rapport_df(akt)

# ## Regnskap skjema0X

# Merknader:
# - ADM i regnskapsfilene er kun data for RHFene
#     - I personellfilen er alle årsverk som ikke er pasientrettet blir regnet som ADM
#

# +
# LAB, PTR, PBF, ADM finnes ikke de andre filene. -> settes til SOM

tjenester_til_SOM = [
    "LAB",
    "PTR",
    "PBF",
    "ADM",
]
m_til_SOM = rgn0x['TJENESTE_KODE'].isin(tjenester_til_SOM)
rgn0x.loc[m_til_SOM, 'TJENESTE_KODE'] = "SOM"
# -

rgn0x = rgn0x[["ORGNR_FRTK", "TJENESTE_KODE", "TOT_UTG", 'HELSEREGION', "ORGNR_STATBANK", "LONN"]]

rgn0x = rgn0x.groupby(['ORGNR_FRTK', 'TJENESTE_KODE', 'HELSEREGION', "ORGNR_STATBANK"]).sum().reset_index()

rgn0x = pd.merge(
    rgn0x,
    foretakstyper,
    on='ORGNR_FRTK',
    how='left'
)

rapport_df(rgn0x)

# ---

# ## Regnskap skjema39

# Hvis `SN07_1 == 86.107`, skal `funksjon`/`TJENESTE` være `REH`
m_86107 = rgn39['SN07_1'] == '86.107'
rgn39.loc[m_86107, 'TJENESTE_KODE'] = "REH"

rgn39 = rgn39[["ORGNR_VIRK", "TOT_UTG", "TJENESTE_KODE", "ORGNR_FRTK", 'FORETAKSTYPE', "HELSEREGION", "ORGNR_STATBANK", "LONN"]]

rgn39 = (
    rgn39.groupby(["ORGNR_FRTK", "TJENESTE_KODE", "FORETAKSTYPE", "HELSEREGION", "ORGNR_STATBANK"])[['TOT_UTG', 'LONN']].sum()
    .reset_index()
)

rapport_df(rgn39)

# ## Personell

per = (
    per[['ORGNR_FRTK', 'TJENESTE_KODE', 'AARSVERK', 'FORETAKSTYPE', 'HELSEREGION', "ORGNR_STATBANK"]]
    .groupby(['ORGNR_FRTK', 'TJENESTE_KODE', 'FORETAKSTYPE', 'HELSEREGION', "ORGNR_STATBANK"]).sum()
    .reset_index()
)

rapport_df(per)

# # Merge alle fire mastertabeller på foretak

dfs = [akt, rgn0x, rgn39, per]

for df in dfs:
    print(df.columns.to_list())

df_final = ft.reduce(
    lambda left, right: pd.merge(
        left, right, on=["ORGNR_FRTK", "TJENESTE_KODE", "FORETAKSTYPE", 'HELSEREGION', "ORGNR_STATBANK"], how="outer"
    ),
    dfs,
)

df_final['TOT_UTG'] = df_final['TOT_UTG_x'].astype(float).fillna(0.0) + df_final['TOT_UTG_y'].astype(float).fillna(0.0)
df_final['LONN'] = df_final['LONN_x'].astype(float).fillna(0.0) + df_final['LONN_y'].astype(float).fillna(0.0)
df_final = df_final.drop(columns=['LONN_x', 'LONN_y', 'TOT_UTG_x', 'TOT_UTG_y'])

df_final[['TOT_UTG', 'LONN']] = df_final[['TOT_UTG', 'LONN']].astype(str).replace("0.0", None).astype(float)

# Ivaretar kolonner der alle verdier er missing.

all_na_kols = [kol for kol in df_final.columns if df_final[kol].isna().sum() == len(df_final)]

df_final = df_final.groupby(['ORGNR_FRTK', 'TJENESTE_KODE', 'FORETAKSTYPE', "HELSEREGION", "ORGNR_STATBANK"]).sum(numeric_only=True, min_count=1).reset_index()

df_final[all_na_kols] = None

df_final

sql_str = hjfunk.lag_sql_str(df_final.ORGNR_FRTK.unique())

sporring_for = f"""
    SELECT ORGNR, NAVN, RECORD_ED, FORETAKS_NR_GDATO
    FROM DSBBASE.SSB_FORETAK
    WHERE
        ORGNR IN {sql_str} AND
        EXTRACT(YEAR FROM RECORD_ED) >= {aar4} AND
        EXTRACT(YEAR FROM FORETAKS_NR_GDATO) <= {aar4}
"""

foretak_navn = hjfunk.les_sql(sporring_for, conn).rename(columns={'ORGNR': 'ORGNR_FRTK'})



df_final = pd.merge(
    df_final,
    foretak_navn,
    on='ORGNR_FRTK',
    how='left'
).copy()

tomt_navn = df_final.NAVN.isna()
df_final.loc[tomt_navn, "NAVN"] = (
    df_final.loc[tomt_navn, "FORETAKSTYPE"]
    + " "
    + df_final.loc[tomt_navn, "TJENESTE_KODE"]
    + " "
    + df_final.loc[tomt_navn, "HELSEREGION"]
)

df_final

df_final = df_final[
    [
        "ORGNR_FRTK",
        'NAVN',
        "TJENESTE_KODE",
        "FORETAKSTYPE",
        "HELSEREGION",
        "ORGNR_STATBANK",
        "UTSKRIVNINGER",
        "OPPHOLDSDOGN",
        "OPPHOLDSDAGER",
        "POLIKLINIKK",
        "DOGNPLASSER",
        "SENGEDOGN",
        "TOT_UTG",
        "AARSVERK",
        "LONN"
    ]
]

verdi_kol = [
    "UTSKRIVNINGER",
    "OPPHOLDSDOGN",
    "OPPHOLDSDAGER",
    "POLIKLINIKK",
    "DOGNPLASSER",
    "SENGEDOGN",
    "TOT_UTG",
    "AARSVERK",
    "LONN"
]



df_final = df_final.sort_values(['ORGNR_FRTK', 'TJENESTE_KODE', 'HELSEREGION', 'FORETAKSTYPE'])



# +
def rapport_missing(df, kol):
    missing = df[kol].isna().sum()
    tom = (df[kol] == '').sum()
    print(f"[{missing + tom}]\tAntall med missing eller tom på {kol}")

def rapport_dublett(df, kols):
    dups = df[kols].duplicated().sum()
    print(f"[{dups}]\tAntall foretak med dublett på {kols}")


# -

rapport_missing(df_final, "NAVN")
rapport_missing(df_final, "ORGNR_FRTK")
rapport_missing(df_final, "TJENESTE_KODE")
rapport_missing(df_final, "HELSEREGION")
rapport_missing(df_final, "ORGNR_STATBANK")
print(100*"-")
rapport_dublett(df_final, ["ORGNR_FRTK", "TJENESTE_KODE"])
rapport_dublett(df_final, ["NAVN", "TJENESTE_KODE"])
rapport_dublett(df_final, ["NAVN", "ORGNR_FRTK", "TJENESTE_KODE", "HELSEREGION", "ORGNR_STATBANK"])



# ## Fordele AOS (administrasjon) utover de andre tjenestene

pd.options.display.float_format = '{:.5f}'.format

df_final_aarsverk = df_final[['ORGNR_FRTK', 'TJENESTE_KODE', 'AARSVERK', 'LONN']].copy()

df_final_aarsverk["LONN_tot"] = (
    df_final_aarsverk
    .groupby(["ORGNR_FRTK"])["LONN"]
    .transform("sum")
)

df_lonn_aos = df_final_aarsverk[df_final_aarsverk['TJENESTE_KODE'] == "AOS"][['ORGNR_FRTK', 'LONN']].copy()

df_final_aarsverk = pd.merge(
    df_final_aarsverk,
    df_lonn_aos,
    how='left',
    on='ORGNR_FRTK',
    suffixes=('', '_aos')
).fillna(0.0)

df_aarsverk_aos = df_final_aarsverk[df_final_aarsverk['TJENESTE_KODE'] == "AOS"][['ORGNR_FRTK', 'AARSVERK']].copy()

df_final_aarsverk = pd.merge(
    df_final_aarsverk,
    df_aarsverk_aos,
    how='left',
    on='ORGNR_FRTK',
    suffixes=('', '_aos')
).fillna(0.0)

df_final_aarsverk['LONN_tot_uten_aos'] = df_final_aarsverk['LONN_tot'] - df_final_aarsverk['LONN_aos']

df_final_aarsverk = df_final_aarsverk.drop(columns=['LONN_aos', 'LONN_tot'])

df_final_aarsverk['andel'] = df_final_aarsverk['LONN'] / df_final_aarsverk['LONN_tot_uten_aos']

df_final_aarsverk['AARSVERK_ny'] = df_final_aarsverk['AARSVERK'] + df_final_aarsverk['AARSVERK_aos'] * df_final_aarsverk['andel']

df_final_aarsverk = df_final_aarsverk[['ORGNR_FRTK', 'TJENESTE_KODE', 'AARSVERK_ny']]



df_final = pd.merge(
    df_final,
    df_final_aarsverk,
    how='left',
    on=['ORGNR_FRTK', 'TJENESTE_KODE']
)

# Tar ut AOS etter å ha fordel aarsverk utover de andre tjenestekodene.

df_final = df_final[df_final['TJENESTE_KODE'] != "AOS"].reset_index(drop=True)

# Lager noen interessante sammenstillinger av tall

df_final['SNITTLONN_millioner_ny'] = df_final['LONN'] / df_final['AARSVERK_ny'] / 1000

df_final['SNITTLONN_millioner'] = df_final['LONN'] / df_final['AARSVERK'] / 1000

df_final['sum_verdier_rad'] = round(df_final[verdi_kol].fillna(0.0).apply(abs).sum(axis=1), 1)


# +
def antall_0_eller_missing(row):
    return ((row != 0.0) & (~row.isnull())).sum()

# Legger til en ny kolonne som teller antall ikke-null og ikke-missing verdier
df_final['ant_num_verdier'] = df_final[verdi_kol].apply(antall_0_eller_missing, axis=1)
# -

# # Lagring

df_final[df_final['NAVN'].str.contains("SYKEHUSBYGG ")]

df_final.info()

excel_ark = {
    'Populasjonsanalyse': df_final
}

if til_lagring:
    idag = pd.Timestamp.today().strftime('%Y-%m-%d-%H%M')
    sti = f"/ssb/stamme01/fylkhels/speshelse/felles/populasjon/{aar4}/populasjonsanalyse_{aar4}_{idag}.xlsx"
    lagre_excel(excel_ark, sti)

idag = pd.Timestamp.today().strftime('%Y-%m-%d-%H%M')

idag
