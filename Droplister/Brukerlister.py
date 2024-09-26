# -*- coding: utf-8 -*-
# # Brukerlister
# Dette er en oversikt over alle foretak som skal ha skjemaer og hvilke skjemaer de skal få. Også kalt _populasjonsliste_ av KOSTRA-IT.
#
# - Alle hjelpeforetak som skal rapportere skal ha skjema: `"0X 0Y 40"`
#     - Dette ser ut til å være alle som ikke er apotek. Foretaksnummerne til disse er listet opp under.
# - Alle RHF skal ha skjema: `"0X 0Y 40 41"`
# - Alle HF skal ha skjema: `"0X 0Y 40 380 440 450 460 48"`
# - Alle foretak med oppdrag og bestillerdokument skal ha skjema: `"39 381 441 451 461 47 48"`
# - Alle private foretak skal ha skjema: `"39 381 441 451 461 47"`

til_lagring = True
lag_passord = True

aar4 = 2024
aar2 = str(aar4)[-2:]
aarfør4 = aar4-1
aarfør2 = str(aarfør4)[-2:]

# +
import pandas as pd
from klass import get_classification
import cx_Oracle
import getpass
import os

# Unngå standardform i output
pd.set_option('display.float_format', lambda x: '%.0f' % x)
# -

import hjelpefunksjoner as hjfunk

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# ## KLASS

HF, RHF, phob, rfss, rfss2, rfss3, rapporteringsenheter = hjfunk.hent_enheter_fra_klass(
    aar4
)

# ## Regionale og felleseide støtteforetak

# rfss skal ikke med, fordi HELSE MIDT-NORGE RHF HELSEPLATTFORMEN ikke skal rapportere. Denne tas ut av klass
RFSS = pd.concat([rfss2, rfss3], ignore_index=True).rename(
    columns={"ORGNR_FORETAK": "ORGNR_FORETAK",
             "NAVN_KLASS": "FORETAK_NAVN"}
)

RFSS

# +
RFSS['SKJEMA_TYPE'] = "0X 0Y 40"

print("Antall: ", RFSS.shape[0])
RFSS.sample(3)
# -

RHF = (
    RHF[['NAVN_KLASS', 'ORGNR_FORETAK']]
    .rename(columns={'NAVN_KLASS': 'FORETAK_NAVN',
                     'ORGNR_FORETAK': 'ORGNR_FORETAK'})
)

RHF['SKJEMA_TYPE'] = "0X 0Y 40 41 48"

print("Antall: ", RHF.shape[0])
RHF.sample(3)

# ## HF

HF = (
    HF.rename(columns={'NAVN_KLASS': 'FORETAK_NAVN'}
             )
    [['ORGNR_FORETAK', 'FORETAK_NAVN']]
)

HF['SKJEMA_TYPE'] = "0X 0Y 40 380 440 450 460 48"
print("Antall: ", HF.shape[0])
HF.sample(3)


# ## Private

def lag_sql_str(arr):
    s = "("
    for nr in arr:
        s += "'" + str(nr) + "',"
    s = s[:-1] + ")"
    s = (
        s.replace("[", "")
         .replace("]", "")
         .replace("''", "'")
    )
    return s


# +
## Henter ORGNR_FORETAK fra 24xx
sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_data = pd.read_sql_query(sporring, conn)
print(f"Fra '24{aar2}'\nRader:    {SFU_data.shape[0]}\nKolonner: {SFU_data.shape[1]}")

foretak = SFU_data[SFU_data['H_VAR2_A'] == 'PRIVAT'][['ORGNR_FORETAK']].to_numpy()

sql_str = lag_sql_str(foretak)

## Henter navn fra SSB_FORETAK-databasen:
sporring_for = f"""
    SELECT *
    FROM DSBBASE.SSB_FORETAK
    WHERE STATUSKODE = 'B' AND ORGNR IN {sql_str}
"""

PRIV = pd.read_sql_query(sporring_for, conn)[['NAVN', 'ORGNR']]
PRIV = PRIV.rename(columns={'NAVN': 'FORETAK_NAVN',
                            'ORGNR': 'ORGNR_FORETAK'})
PRIV['SKJEMA_TYPE'] = "39 381 441 451 461 47"

print("\nFra DSBBASE.SSB_FORETAK\nAntall foretak: ", PRIV.shape[0])
PRIV.sample(3)
# -

# ## Oppdrags- og bestillerdokument

phob

PHOB = (
    phob.rename(columns={
        'NAVN_KLASS': 'FORETAK_NAVN'
    })
    [['ORGNR_FORETAK', 'FORETAK_NAVN']]
).copy()

# +
PHOB['SKJEMA_TYPE'] = "39 381 441 451 461 47 48"

print("Antall: ", PHOB.shape[0])
PHOB.sample(3)
# -

# DIAKONHJEMMET SKAL HA 0X istedenfor 39
PHOB.loc[PHOB['ORGNR_FORETAK'] == "982791952", 'SKJEMA_TYPE'] = "0X 381 441 451 461 47 48"

# ## Slå sammen til en dataframe som eksporteres til `.csv`

# +
brukerliste_dfs = [RFSS, RHF, HF, PRIV, PHOB]
brukerliste_df = pd.concat(brukerliste_dfs)
print("Antall foretak i brukerlisten: ", brukerliste_df.shape[0])


brukerliste_df['FORETAK_NAVN'] = brukerliste_df['FORETAK_NAVN'].str.upper()

brukerliste_df = brukerliste_df.reset_index(drop=True)
# -


brukerliste_df.sample(3)

# Dublettsjekk på foretaksnummer (ORGNR_FRTK)
assert brukerliste_df.ORGNR_FORETAK.duplicated().sum() == 0

dato_idag = pd.Timestamp("today").strftime("%d%m%y")
filnavn = "Brukerliste" + "_" + str(aar4) + "_" + dato_idag + ".csv"
stamme = "/ssb/stamme01/fylkhels/speshelse/felles/droplister/"
sti_til_lagring = stamme + str(aar4) + "/" + str(dato_idag) + "/"
sti = sti_til_lagring + filnavn
if til_lagring:
    directory = os.path.dirname(sti)

    if not os.path.exists(directory):
        os.makedirs(directory)
    
    brukerliste_df.to_csv(
                      sti,
                      sep=";", 
                      encoding='latin1', 
                      index=False
                     )
    print(f"Filen er lagret på {sti_til_lagring + filnavn}")

# # Generere passord-filer

pass_stamme = (
        "/ssb/stamme01/fylkhels/speshelse/felles/brukere/"
        + str(aar4)
)
pass_sti = pass_stamme + r"/password_master.csv"

if not os.path.exists(pass_stamme):
    print("Lager årgangsmappe")
    os.makedirs(pass_stamme)

if not os.path.exists(pass_sti):
    print("Ingen passordfil finnes. Oppretter fil:")
    empty_df = pd.DataFrame(columns=["Description", "Username", "Password"])
    empty_df.to_csv(pass_sti, sep=";", encoding='latin1', index=False)
    print("Opprettet ny tom passordfil")



passord_master = pd.read_csv(pass_sti, dtype='object', sep=";", encoding='latin1')

passord_master.info()


def get_new_password(dummy_var):
    from random import randint
    
    return randint(10**7, 10**8-1)
# +
skjema_type_unik = list(brukerliste_df.SKJEMA_TYPE.unique())

for skj in skjema_type_unik:
    skjema_type_df = brukerliste_df[brukerliste_df["SKJEMA_TYPE"] == skj].copy()
    ant_enheter = len(skjema_type_df)

    skjema_type_df = skjema_type_df.rename(
        columns={
            "FORETAK_NAVN": "Description",
            "ORGNR_FORETAK": "Username",
        }
    ).drop(columns="SKJEMA_TYPE")

    
    skjema_type_df = pd.merge(
        skjema_type_df,
        passord_master[['Username', 'Password']],
        on='Username',
        how='left',
    )
    
    m_har_ikke_passord = skjema_type_df['Password'].isna()
    
    skjema_type_df.loc[m_har_ikke_passord, 'Password'] = (
        skjema_type_df[m_har_ikke_passord]['Password']
        .apply(get_new_password)
    )
    
    skjema_type_df = skjema_type_df[['Description', 'Username', 'Password']]
    
    til_master =  skjema_type_df[m_har_ikke_passord].copy()
    passord_master = pd.concat([passord_master, til_master], ignore_index=True)
    print(f"Det er skrevet {len(til_master)} rader til passord_master. Det er tilsammen nå {len(passord_master)} passord.")
    
    skj_str = skj.replace(" ", "_")
    filnavn = "prod_users_" + skj_str + "_" + str(dato_idag) + ".csv"

    stamme = (
        "/ssb/stamme01/fylkhels/speshelse/felles/brukere/"
        + str(aar4)
        + "/"
        + str(dato_idag)
        + "/"
    )
    sti_prod = stamme + "prod/"
    sti_test = stamme + "test/"
    
    if lag_passord:
        # --- Prod
        if not os.path.exists(sti_prod):
            os.makedirs(sti_prod)
        skjema_type_df.to_csv(
            sti_prod + filnavn, sep=";", encoding="latin1", index=False
        )
        print(f"Brukerfil lagret på {sti_prod + filnavn}")
        
        # --- Test
        if not os.path.exists(sti_test):
            os.makedirs(sti_test)
        skjema_type_df['Password'] = 12341234
        skjema_type_df.to_csv(
            sti_test + filnavn, sep=";", encoding="latin1", index=False
        )
        print(f"Testbrukerfil lagret på {sti_test + filnavn}")
        passord_master.to_csv(pass_sti, sep=";", encoding='latin1', index=False)
        print(80*"-")
        print("Lagret passord masterfil.")
        print(80*"-")
        
# -

