# +
import pandas as pd
import cx_Oracle
from db1p import query_db1p
import getpass
import datetime as dt

til_lagring = False
# -

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# +
aar4 = 2024
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

# # Skjema: rapporteringsoversikt

# ## SFU: `dsbbase.dlr_enhet_i_delreg `
# På enhetsnivå

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
""" 
SFU_enhet = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_enhet.shape[0]}\nKolonner: {SFU_enhet.shape[1]}")
SFU_enhet = SFU_enhet[SFU_enhet['ORGNR'].notnull()]
SFU_enhet = SFU_enhet[SFU_enhet['KVITT_TYPE'].isnull()]
print(f"Etter å ha tatt ut nullverdier i ORGNR og virksomheter som er kvittert ut:")
print(f"Rader:    {SFU_enhet.shape[0]}")


# ## Skjema-SFU: `dsbbase.dlr_enhet_i_delreg_skjema `

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_skjema = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_skjema.shape[0]}\nKolonner: {SFU_skjema.shape[1]}")


# ## Kobler SFU-enhet og SFU-skjema

SFU = pd.merge(
    SFU_skjema,
    SFU_enhet,
    how='left',
    on='IDENT_NR',
    suffixes=("_skj","_enh")
)

# # Prosentandel av virksomheter som har levert
#
# Alle skjema i SFU med antall foremkomster på virksomhetsnivå:

# +
tot = (
    SFU
    .SKJEMA_TYPE_skj
    .value_counts()
    .to_dict()
)

ant_ikke_levert = (
    SFU[
        SFU.KVITT_TYPE_skj
        .isna()
    ]
    .SKJEMA_TYPE_skj
    .value_counts()
    .to_dict()
)

skjemaoversikt = pd.DataFrame([tot, ant_ikke_levert]).T
skjemaoversikt.columns = ['tot', 'ikke_levert']
skjemaoversikt['levert'] = skjemaoversikt['tot'] - skjemaoversikt['ikke_levert']
skjemaoversikt['prosentandel_levert'] = round(skjemaoversikt['levert'] / skjemaoversikt['tot'] * 100)
skjemaoversikt = skjemaoversikt.sort_values('prosentandel_levert',
                                            ascending=False)
skjemaoversikt
# -

# # Foretak med oppdrags- og bestillerdokument

dh12 = SFU[(SFU['H_VAR2_A'] == 'OPPDRAG') & (SFU['SKJEMA_TYPE_skj'] != 'HELSE39')]

dh12 = dh12[['NAVN', 'SKJEMA_TYPE_skj', 'ORGNR', 'ORGNR_FORETAK', 'KVITT_TYPE_enh', 'KVITT_TYPE_skj']]


# Legger på foretaksnavn via VoF

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
sql_str = lag_sql_str(dh12['ORGNR_FORETAK'].unique())

sporring_for = f"""
    SELECT *
    FROM DSBBASE.SSB_FORETAK
    WHERE STATUSKODE = 'B' AND ORGNR IN {sql_str}
"""

navn_foretak = pd.read_sql_query(sporring_for, conn)[['NAVN', 'ORGNR']]

navn_foretak.columns = ['NAVN_FORETAK', 'ORGNR_FORETAK']

dh12 = (
    pd.merge(
        dh12,
        navn_foretak,
        how='left',
        on='ORGNR_FORETAK',
    )
)

# +
alle = pd.pivot_table(dh12[['NAVN_FORETAK',
                            'SKJEMA_TYPE_skj'
                           ]],
                       index='NAVN_FORETAK',
                       columns=['SKJEMA_TYPE_skj'],
                       aggfunc=len,
                      )

K = (
    pd.pivot_table(dh12[['NAVN_FORETAK',
                         'KVITT_TYPE_skj',
                         'SKJEMA_TYPE_skj',
                       ]],
                   index='NAVN_FORETAK',
                   columns=['KVITT_TYPE_skj', 'SKJEMA_TYPE_skj'],
                   aggfunc=len,
                   fill_value=0
                   )
    .astype(float)
    .droplevel(axis=1, level=0)
)
# -

levsum = K.sum(axis=1)
totsum = alle.sum(axis=1)

# +
df = (
    pd.merge(
        K,
        pd.DataFrame(levsum,columns=['SUM LEVERT']),
        how='left',
        on='NAVN_FORETAK'
    )
)

df = (
    pd.merge(
        df,
        pd.DataFrame(totsum,columns=['SUM FORVENTET']),
        how='left',
        on='NAVN_FORETAK'
    )
)

# +
df['ANDEL RAPPORTERT'] = df['SUM LEVERT'] / df['SUM FORVENTET'] * 100

df
# -






