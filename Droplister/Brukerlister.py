# -*- coding: utf-8 -*-
# # Brukerlister
#
#
# Dette utgår:
# Dette er en oversikt over alle foretak som skal ha skjemaer og hvilke skjemaer de skal få. Også kalt _populasjonsliste_ av KOSTRA-IT.
#
# - Alle hjelpeforetak som skal rapportere skal ha skjema: `"0X 0Y 40"`
#     - Dette ser ut til å være alle som ikke er apotek. Foretaksnummerne til disse er listet opp under.
# - Alle RHF skal ha skjema: `"0X 0Y 40 41"`
# - Alle HF skal ha skjema: `"0X 0Y 40 380 440 450 460 48"`
# - Alle foretak med oppdrag og bestillerdokument skal ha skjema: `"39 381 441 451 461 47 48"`
# - Alle private foretak skal ha skjema: `"39 381 441 451 461 47"`

til_lagring = False
lag_passord = False

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

from sqlalchemy import create_engine

username = getpass.getuser()
dsn = "DB1P"
try:
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")
    conn = engine.connect()
except:
    print("Passord ikke skrevet inn")
    password = getpass.getpass(prompt='Oracle-passord: ')
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")
    conn = engine.connect()

# +
# conn.close()
# -

# # DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
# Inneholder enheter som skal svare på skjema

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_data = hjfunk.les_sql(sporring, conn)
print(f"Fra '24{aar2}'\nRader:    {SFU_data.shape[0]}\nKolonner: {SFU_data.shape[1]}")

sporring = f"""
    SELECT DELREG_NR, IDENT_NR, ENHETS_TYPE, SKJEMA_TYPE
    FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_skj = hjfunk.les_sql(sporring, conn)
print(f"Fra '24{aar2}'\nRader:    {SFU_skj.shape[0]}\nKolonner: {SFU_skj.shape[1]}")

SFU_skj = SFU_skj[SFU_skj['SKJEMA_TYPE'] != 'RA-0595'].copy()

skjema_mapping = {
    "HELSE47": '47',
    "HELSE44P": '441',
    "HELSE45P": '451',
    "HELSE38P": '381',
    "HELSE39": '39',
    "HELSE46P": '461',
    "HELSE0X": '0X',
    "HELSE48": '48',
    "HELSE0Y": '0Y',
    "HELSE40": '40',
    "HELSE41": '41',
    "HELSE38O": '380',
    "HELSE44O": '440',
    "HELSE45O": '450',
}

SFU_skj['SKJEMA_TYPE_NY'] = SFU_skj['SKJEMA_TYPE'].map(skjema_mapping)

SFU_skj = SFU_skj.sort_values('SKJEMA_TYPE_NY')

# resultat = SFU_skj.groupby(['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE'])['SKJEMA_TYPE_NY'].apply(lambda x: ' '.join(x)).reset_index()
resultat = SFU_skj.groupby(['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE'])['SKJEMA_TYPE_NY'].unique().reset_index()

resultat['SKJEMA_TYPE_NY_NUM'] = resultat.SKJEMA_TYPE_NY.apply(lambda x: ("_").join(x))





brukerliste = pd.merge(
    resultat,
    SFU_data[['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE', 'NAVN', 'ORGNR', 'ORGNR_FORETAK']],
    on=['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE']
)

brukerliste[brukerliste['ORGNR_FORETAK'] == '941455077']

brukerliste[brukerliste['ORGNR_FORETAK'] == '980693732']


brukerliste[brukerliste['ORGNR_FORETAK'] == '916269331']




















# ---

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

