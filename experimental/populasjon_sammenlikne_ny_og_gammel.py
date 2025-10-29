import pandas as pd
from glob import glob

import os

from fagfunksjoner import ProjectRoot

# +
import pandas as pd

from sqlalchemy import create_engine

import getpass
import os
# -

with ProjectRoot():
    import Droplister.hjelpefunksjoner as hjfunk

username = getpass.getuser()
dsn = "DB1P"
try:
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")
except:
    print("Passord ikke skrevet inn")
    password = getpass.getpass(prompt='Oracle-passord: ')
    engine = create_engine(f"oracle+cx_oracle://{username}:{password}@{dsn}")

# # Populasjonsanalyse

df1 = pd.read_excel('/ssb/stamme01/fylkhels/speshelse/felles/populasjon/2024/populasjonsanalyse_2024_2025-09-22-1049.xlsx')

df2 = pd.read_excel('/home/mfm/data_temp/Private_med_RHFavtale.xlsx', dtype={"Orgnummer": str})

df2.groupby("RHF")["Orgnummer"].size()

sammen_df = pd.merge(
    df1,
    df2,
    left_on="ORGNR_FRTK",
    right_on="Orgnummer",
    how="outer",
    indicator=True
)

sammen_df["_merge"].value_counts()

sammen_df[sammen_df["_merge"] == "right_only"]

# # SFU

conn = engine.connect()

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('2425')
"""
SFU_data = hjfunk.les_sql(sporring, conn)
print(f"Rader:    {SFU_data.shape[0]}\nKolonner: {SFU_data.shape[1]}")
SFU_data.info()

[col for col in SFU_data.columns if "NR" in col]

SFU_priv = SFU_data[SFU_data["H_VAR2_A"].isin(["PRIVAT", "OPPDRAG"])][["DELREG_NR", "ENHETS_TYPE", "ORGNR", "ORGNR_FORETAK", "SKJEMA_TYPE", "NAVN"]]

SFU_sammen = pd.merge(
    df2,
    SFU_data,
    left_on="Orgnummer",
    right_on="ORGNR_FORETAK",
    how="outer",
    indicator=True
)

SFU_data[SFU_data["ORGNR_FORETAK"] == "923393528"]

SFU_sammen[SFU_sammen["_merge"] == "left_only"].dropna(axis=1)

df1[df1["ORGNR_FRTK"] == "999609635"]



# # Nytt utrekk

df_ny = pd.read_csv("/home/mfm/data_temp/Private_med_RHFavtale.csv", sep=",", dtype="object")

df_ny["Orgnummer"] = df_ny["Orgnummer"].str.replace(" ", "")

SFU_sammen = pd.merge(
    df_ny,
    SFU_data,
    left_on="Orgnummer",
    right_on="ORGNR_FORETAK",
    how="outer",
    indicator=True
)

nye = pd.merge(
    df_ny,
    SFU_data,
    left_on="Orgnummer",
    right_on="ORGNR_FORETAK",
    how="outer",
    indicator=True
)

df_ut1 = nye[nye["_merge"] == "left_only"].dropna(axis=1).copy()

print(df_ut1.drop(columns=["_merge"]).to_markdown())

df_ut2

df_ut2 = SFU_sammen[SFU_sammen["_merge"] == "left_only"].dropna(axis=1).copy()

pd.concat([df_ut1, df_ut2]).drop(columns=["_merge"])


