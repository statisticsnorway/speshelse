# # Brukerlister
# Dette er en oversikt over alle foretak som skal ha skjemaer og hvilke skjemaer de skal få. Også kalt _populasjonsliste_ av KOSTRA-IT.
#
# - Alle hjelpeforetak som skal rapportere skal ha skjema: `"0X 0Y 40"`
#     - Dette ser ut til å være alle som ikke er apotek. Foretaksnummerne til disse er listet opp under.
# - Alle RHF skal ha skjema: `"0X 0Y 40 41"`
# - Alle HF skal ha skjema: `"0X 0Y 40 380 440 450 460 48"`
# - Alle foretak med oppdrag og bestillerdokument skal ha skjema: `"39 381 441 451 461 47 48"`
# - Alle private foretak skal ha skjema: `"39 381 441 451 461 47"`

aar4 = 2022
aar2 = str(aar4)[-2:]

# +
import pandas as pd
from klass import klass_df
from klass import klass_df_wide
from klass import klass_get
import cx_Oracle
import getpass
import requests

# Fjerner begrensning på antall rader og kolonner som vises av gangen
pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

# Unngå standardform i output
pd.set_option('display.float_format', lambda x: '%.0f' % x)
# -

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# ## Hjelpeforetak

hjelpeforetak_som_ikke_er_rapporteringsenhet = ['918098275', '983974716', '983974805', '983974937', '992281618']

URL = f'https://data.ssb.no/api/klass/v1/classifications/605/codes.json?from={aar4}-01-01&includeFuture=True'
regfel_df = klass_df(URL, level='codes')

# +
# tar ut RHFene fra listen
df_hj = regfel_df.query("level != '1' and parentCode not in ['03','04','05','12']") 

# tar bort unntakene
df_hj = df_hj[~df_hj['code'].isin(hjelpeforetak_som_ikke_er_rapporteringsenhet)]
# -

df_hj['s'] = "0X 0Y 40"
df_hj = df_hj[['name', 'code', 's']]
df_hj.columns = ['FORETAK_NAVN', 'ORGNR_FORETAK', 'SKJEMA_TYPE']

df_hj

# ## RHF

df_rhf = regfel_df[regfel_df['parentCode'].isin(['03','04','05','12'])].copy()

df_rhf['s'] = "0X 0Y 40 41"
df_rhf = df_rhf[['name', 'code', 's']]
df_rhf.columns = ['FORETAK_NAVN', 'ORGNR_FORETAK', 'SKJEMA_TYPE']

df_rhf

# ## HF

URL = f'https://data.ssb.no/api/klass/v1/classifications/603/codes.json?from={aar4}-01-01&includeFuture=True'
df_hf = klass_df_wide(URL)

df_hf = df_hf[['code_3', 'name_3']]
df_hf.columns = ['ORGNR_FORETAK', 'FORETAK_NAVN']

df_hf['SKJEMA_TYPE'] = "0X 0Y 40 380 440 450 460 48"
df_hf

# ## Private

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_data = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_data.shape[0]}\nKolonner: {SFU_data.shape[1]}")
SFU_data.info()

foretak = SFU_data[SFU_data['H_VAR2_A'] == 'PRIVAT'][['ORGNR_FORETAK']].to_numpy()


def lag_sql_str(arr):
    s = "("
    for nr in arr:
        s += "'" + str(nr) + "',"
    s = s[:-1] + ")"
    return s


sql_str = (
    lag_sql_str(foretak)
    .replace("[", "")
    .replace("]", "")
    .replace("''", "'")
)

sporring_for = f"""
    SELECT *
    FROM DSBBASE.SSB_FORETAK
    WHERE STATUSKODE = 'B' AND ORGNR IN {sql_str}
"""
df_p = pd.read_sql_query(sporring_for, conn)[['NAVN', 'ORGNR']]

df_p['s'] = "39 381 441 451 461 47"

df_p.columns = ['FORETAK_NAVN', 'ORGNR_FORETAK', 'SKJEMA_TYPE']

print("Antall: ", df_p.shape)
df_p.head(5)

# ## Oppdrags- og bestillerdokument

URL = f'https://data.ssb.no/api/klass/v1/classifications/604/codes.json?from={aar4}-01-01&includeFuture=True'
df_op = klass_df_wide(URL)

# +
df_op = df_op[['name_2', 'code_2']]

df_op.loc[:, 's'] = "39 381 441 451 461 47 48"
df_op.columns = ['FORETAK_NAVN', 'ORGNR_FORETAK', 'SKJEMA_TYPE']
print("Antall: ", df_op.shape)
df_op.head(5)
# -

# ## Slå sammen til en dataframe som eksporteres til `.csv`

brukerliste_dfs = [df_hj, df_rhf, df_hf, df_p, df_op]
brukerliste_df = pd.concat(brukerliste_dfs)
print("Antall: ", brukerliste_df.shape)
brukerliste_df.sample(5)


brukerliste_df

import os

sammenlikne = pd.read_csv(r"Brukerliste_2022_061222.csv", encoding="latin1", sep=";")

sammenlikne

tot = pd.merge(
    brukerliste_df,
    sammenlikne,
)

