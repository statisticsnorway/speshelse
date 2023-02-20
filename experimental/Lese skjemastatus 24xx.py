import pandas as pd
import cx_Oracle
from db1p import query_db1p
import getpass
# import os
# import requests

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# +
aar4 = 2022
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

# # Vanlig SFU: `dsbbase.dlr_enhet_i_delreg `
# På enhetsnivå

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
""" 
SFU_enhet = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_enhet.shape[0]}\nKolonner: {SFU_enhet.shape[1]}")
SFU_enhet.info()


# Enheter med verdi i 'KVITT_TYPE' filtreres ut (de er nedlagte enheter). Fjerner også enheter uten ORGNR
# SFU_enhet = SFU_enhet[~SFU_enhet['KVITT_TYPE'].notnull()]
SFU_enhet = SFU_enhet[SFU_enhet['ORGNR'].notnull()]

# # Skjema-SFU: `dsbbase.dlr_enhet_i_delreg_skjema `

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_skjema = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_skjema.shape[0]}\nKolonner: {SFU_skjema.shape[1]}")
SFU_skjema.info()


SFU = pd.merge(
    SFU_skjema,
    SFU_enhet,
    how='left',
    on='IDENT_NR',
    suffixes=("_skj","_enh")
)

SFU.sample(3)

SFU[['NAVN', 'ORGNR', 'SKJEMA_TYPE_skj', 'KVITT_TYPE_skj', 'KVITT_TYPE_enh']].sample(3)

# Vi skal ha purring på de som er:
#
# - [x] ikke har `HELSE39` i `SKJEMA_TYPE` i SFU_skjema (ta bort)
# - [x] `HELSE48` **HVIS** foretak er privat. Alle andre skjema skal purres på
# - [ ] blanke på `KVITT_TYPE` i SFU_skjema
#
#

# ### Velger alle private skjema:
# > Merk at 39 er privat, men har senere leveringsfrist. Filtreres ut allerede her

# +
skjema_til_purring = ['HELSE38P', 'HELSE44P', 'HELSE45P',
                      'HELSE46P', 'HELSE47', 'HELSE48']

purring_df = SFU[SFU['SKJEMA_TYPE_skj'].isin(skjema_til_purring)]
# -

# ### Tar bort de som skal levere skjema48 og ikke er private:

maske1 = (purring_df['SKJEMA_TYPE_skj'] == 'HELSE48') & (purring_df['H_VAR2_A'] != 'OPPDRAG')
purring_df = purring_df[~maske1]

# ### Tar bort alle som ikke har noe oppført på `KVITT_TYPE_skj` 

purring_df = purring_df[purring_df['KVITT_TYPE_skj'].isna()]

purring_df[['NAVN', 'ORGNR', 'SKJEMA_TYPE_skj', 'KVITT_TYPE_skj']].sample(3)

purring_df.shape

#

foretak_til_purring = purring_df.ORGNR_FORETAK.unique()

len(foretak_til_purring)

# # Altinn-delreg 2087722

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar2}')
""" 
altinn = pd.read_sql_query(sporring, conn)
print(f"Rader:    {altinn.shape[0]}\nKolonner: {altinn.shape[1]}")
altinn.info()


foretak_i_altinn_df = altinn[altinn.ORGNR.isin(foretak_til_purring)].ORGNR.to_numpy()

set(foretak_til_purring) - set(foretak_i_altinn_df)

set(foretak_i_altinn_df) - set(foretak_til_purring)

SFU[SFU['ORGNR'].isin(['922602344', '925001333'])]

(
    purring_df[
        purring_df[
            'ORGNR_FORETAK'
        ]
        .isin(
            ['922602344', '925001333']
        )
    ]
    [
        ['NAVN', 'NAVN1', 'NAVN2',
         'ORGNR', 'ORGNR_FORETAK',
         'SKJEMA_TYPE_skj',
         'KVITT_TYPE_skj'
        ]
    ]
)
