import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine
import getpass
import datetime as dt

# +
aar4 = 2024
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

import sys

sys.path.append("../Droplister")

import hjelpefunksjoner as hjfunk

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

n = aar4 - 3

siste_år = [x for x in range(n, aar4 + 1)]

siste_år

df_sammen = pd.DataFrame()
for år in siste_år:
    år2 = str(år)[-2:]
    sporring = f"""
        SELECT *
        FROM DYNAREV.VW_SKJEMA_DATA
        WHERE DELREG_NR IN ('24{år2}', '19377{år2}') AND SKJEMA IN ('HELSE47') AND AKTIV = '1'
    """
    df_temp = hjfunk.les_sql(sporring, conn)
    # print(år)
    # print(df_temp['FELT_ID'].unique())
    df_temp = (
        df_temp[["ENHETS_TYPE", "ENHETS_ID", "FELT_ID", "FELT_VERDI"]]
        .pivot(index="ENHETS_ID", columns="FELT_ID", values="FELT_VERDI")
        .reset_index()
    )
    df_temp = df_temp[["AARGANG", "FINST_ORGNR", "SDGN_SUM", "D_HELD"]]
    df_sammen = pd.concat([df_sammen, df_temp])

df_sammen

df_sammen[df_sammen['FINST_ORGNR'] == '974830191']

df_sammen['SDGN_SUM'] = pd.to_numeric(df_sammen['SDGN_SUM'])

df_sammen['D_HELD'] = pd.to_numeric(df_sammen['D_HELD'])

sengedøgn = df_sammen.groupby('FINST_ORGNR').agg(
    sdgn_max_siste_år=('SDGN_SUM','max'),
    sdgn_min_siste_år=('SDGN_SUM','min'),
    sdgn_mean_siste_år=('SDGN_SUM','mean'),
    sdgn_median_siste_år=('SDGN_SUM','median'),
    sdgn_unique_siste_år=('SDGN_SUM','unique'),
).reset_index()

sengedøgn

sporring = f"""
    SELECT *
    FROM DYNAREV.VW_SKJEMA_DATA
    WHERE DELREG_NR IN ('24{aar2}', '19377{aar2}') AND SKJEMA IN ('HELSE47') AND AKTIV = '1'
"""

df = hjfunk.les_sql(sporring, conn)

df = df.pivot(index='ENHETS_ID', columns='FELT_ID', values='FELT_VERDI').reset_index()

df = pd.merge(
    df,
    sengedøgn,
    on='FINST_ORGNR',
    how='left'
)

m1 = df['D_HELD'].notna()
m2 = df['SDGN_SUM'].notna()
df = df[m1 & m2]

df['SDGN_SUM'] = pd.to_numeric(df['SDGN_SUM'])

df['diff'] = df['sdgn_max_siste_år'] - df['SDGN_SUM']



df.sort_values("sdgn_min_siste_år", ascending=True)[
    [
        "FINST_NAVN",
        "FINST_ORGNR",
        "sdgn_max_siste_år",
        "sdgn_min_siste_år",
        "sdgn_mean_siste_år",
        "sdgn_median_siste_år",
        "sdgn_unique_siste_år",
        "diff",
    ]
]

df1 = df[['FINST_NAVN', 'FINST_ORGNR', 'D_HELD', 'SDGN_SUM', 'sdgn_unique_siste_år']].sort_values('SDGN_SUM').copy()

for_lave_sdgn = ["974116464", "973254618", "924212446"]

df[['MERKNAD0', 'MERKNAD1']].iloc[34]

df[df['FINST_ORGNR'].isin(for_lave_sdgn)][['MERKNAD0', 'MERKNAD1']]

for_lave_dheld = ["912041379",
"970981071",
"975984168",
"983478778",
"971436875",
"999087345",]

pd.set_option('display.max_colwidth', None)

df[df['FORETAKETS_ORGNR'].isin(for_lave_dheld)][['MERKNAD0', 'MERKNAD1']]







df = pd.DataFrame()
for aar in siste_år:
    print(aar)
    sporring = f"""
    SELECT *
    FROM KOSTRA_EXP.HELSE47_{aar}
    """
    df_temp = hjfunk.les_sql(sporring, conn)
    df_temp = df_temp[['SSB_LOGDATO', 'FORETAKETS_ORGNR', 'FINST_ORGNR', 'SDGN_SUM', 'D_HELD']]
    df_temp['ÅRGANG'] = aar
    df = pd.concat([df, df_temp])

df = df.drop_duplicates()

df.shape

df = df.dropna(axis=0, how='any')

df = df.sort_values(['FINST_ORGNR', 'SSB_LOGDATO'], ascending=(True, False))

dubletter = df.duplicated(subset=['FINST_ORGNR', 'ÅRGANG'], keep='first')

df = df[~dubletter]

df

df_grp = df.groupby(['FORETAKETS_ORGNR', 'FINST_ORGNR']).agg(
    SDGN_mean=('SDGN_SUM', 'mean'),
    SDGN_median=('SDGN_SUM', 'median'),
    SDGN_max=('SDGN_SUM', 'max'),
    SDGN_min=('SDGN_SUM', 'min'),
    # D_HELD=('D_HELD', 'first')
).sort_values('SDGN_mean', ascending=False)



df_grp.style.background_gradient(cmap='Blues').format("{:.0f}")

df_grp2 = df.groupby(['FORETAKETS_ORGNR', 'FINST_ORGNR']).agg(
    D_HELD_mean=('D_HELD', 'mean'),
    D_HELD_median=('D_HELD', 'median'),
    D_HELD_max=('D_HELD', 'max'),
    D_HELD_min=('D_HELD', 'min'),
    # D_HELD=('D_HELD', 'first')
).sort_values('D_HELD_mean', ascending=False)

df_grp2.style.background_gradient(cmap='Blues').format("{:.0f}")





sporring = f"""
SELECT *
FROM KOSTRA_EXP.HELSE47_{aar4}
"""
df_siste = hjfunk.les_sql(sporring, conn)


df_siste.sample(1).T

df_siste[df_siste['FINST_ORGNR'] == '997808347']



df_siste.FINST_ORGNR.value_counts()

df_siste.columns

df_siste[df_siste.duplicated(subset=['FINST_ORGNR'], keep=False)][['FINST_ORGNR']]



df_siste = df_siste[['FORETAKETS_NAVN', 'FORETAKETS_ORGNR', 'FINST_NAVN', 'FINST_ORGNR', 'FINST_INKL', 'HELSEREGION_UTFYLT', 'HELSEREGION_EPOST', 'MERKNAD0', 'SDGN_SUM', 'SDGN_HT_FJOR', 'JANEI', 'D_HELD', 'D_HELD_IFJOR']]
