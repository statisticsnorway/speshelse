import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine
import getpass
import datetime as dt

from vaskify import Detect

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

antall_årganger = 2
n = aar4 - (antall_årganger -1)
siste_år = [x for x in range(n, aar4 + 1)]
print(siste_år)

df = pd.DataFrame()
for år in siste_år:
    år2 = str(år)[-2:]
    sporring = f"""
        SELECT *
        FROM DYNAREV.VW_SKJEMA_DATA
        WHERE DELREG_NR IN ('24{år2}', '19377{år2}') AND SKJEMA IN ('HELSE47') AND AKTIV = '1'
    """
    df_temp = hjfunk.les_sql(sporring, conn)
    print(år)
    print(df_temp['FELT_ID'].unique())
    df_temp = (
        df_temp[["ENHETS_TYPE", "ENHETS_ID", "FELT_ID", "FELT_VERDI"]]
        .pivot(index=["ENHETS_ID"], columns="FELT_ID", values="FELT_VERDI")
        .reset_index()
    )
    df_temp = df_temp[["AARGANG", "FINST_ORGNR", "SDGN_SUM", "D_HELD", "FORETAKETS_NAVN", "MERKNAD0", "MERKNAD1"]]
    df = pd.concat([df, df_temp])

df['SDGN_SUM'] = pd.to_numeric(df['SDGN_SUM'])
df['D_HELD'] = pd.to_numeric(df['D_HELD'])

df['AARGANG'] = pd.to_datetime(df['AARGANG'], format='%d.%m.%Y').dt.year.astype(str)

df['AARGANG'] = df['AARGANG'].astype(str)
df['AARGANG'] = df['AARGANG'] + "-01"


def show_deviant_records(detect_df, df):
    display(detect_df.style.set_caption("Flaggede records"))
    display(
        pd.merge(
            df,
            detect_df['FINST_ORGNR'],
            how='inner',
            on='FINST_ORGNR'
        ).sort_values('FINST_ORGNR')
    )



det = Detect(df, id_nr="FINST_ORGNR")

# ## Sengedøgn `SDGN_SUM`

tusen1 = det.thousand_error(y_var="SDGN_SUM", time_var="AARGANG").query("flag_thousand == 1.0")
akkum1 = det.accumulation_error(y_var="SDGN_SUM", time_var="AARGANG").query("flag_accumulation == 1.0")
hbmet1 = det.hb(y_var="SDGN_SUM", time_var="AARGANG").query("flag_hb == 1.0")

if len(tusen1) > 1:
    show_deviant_records(tusen1, df)
else:
    print("Ingen tusenfeil oppdaget")

if len(akkum1) > 1:
    show_deviant_records(akkum1, df)
else:
    print("Ingen akkumuleringsfeil oppdaget")

if len(hbmet1) > 1:
    show_deviant_records(hbmet1, df)
else:
    print("Ingen records flagget med HB-metoden")

# ## Døgnplasser `D_HELD`

tusen2 = det.thousand_error(y_var="D_HELD", time_var="AARGANG").query("flag_thousand == 1.0")
akkum2 = det.accumulation_error(y_var="D_HELD", time_var="AARGANG").query("flag_accumulation == 1.0")
hbmet2 = det.hb(y_var="D_HELD", time_var="AARGANG").query("flag_hb == 1.0")

if len(tusen2) > 1:
    show_deviant_records(tusen2, df)
else:
    print("Ingen tusenfeil oppdaget")

if len(akkum2) > 1:
    show_deviant_records(akkum2, df)
else:
    print("Ingen akkumuleringsfeil oppdaget")

if len(hbmet2) > 1:
    show_deviant_records(hbmet2, df)
else:
    print("Ingen records flagget med HB-metoden")










