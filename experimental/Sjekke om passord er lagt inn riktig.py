# +
import pandas as pd
from sqlalchemy import create_engine
import getpass

user = getpass.getuser()
pw = getpass.getpass("Passord: ")
tns = "DB1P"  # Forutsetter at dette er definert i tnsnames.ora

engine = create_engine(f'oracle+cx_oracle://{user}:{pw}@{tns}')

# +
aar4 = 2024
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -



delregistre = {
    "priv": f"20877{aar2}",
    "hf": f"138555{aar2}",
    "støtt": "25428",
    "phob": "25468" 
}

komm = {}

for navn, delreg in delregistre.items():
    sporring = f"""
        SELECT *
        FROM DSBBASE.DLR_ENHET_I_DELREG
        WHERE DELREG_NR IN ('{delreg}')
    """
    df = pd.read_sql_query(sporring, engine)

    df.columns = [col.upper() for col in df.columns]
    komm[navn] = df

komm['priv'][['NAVN', 'ORGNR', 'H_VAR2_A']]


