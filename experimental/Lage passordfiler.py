import pandas as pd
import cx_Oracle
from db1p import query_db1p
import getpass

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# +
aar4 = 2024
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -



# ## Importer passordfil

pass_stamme = (
        "/ssb/stamme01/fylkhels/speshelse/felles/brukere/"
        + str(aar4)
)
pass_sti = pass_stamme + r"/password_master.csv"

passord = pd.read_csv(pass_sti, encoding='latin1', sep=';')

passord.head(3)

# ## Siste brukerliste

stamme = (
        "/ssb/stamme01/fylkhels/speshelse/felles/brukere/"
        + str(aar4)
        + "/*")

import glob

dfs = []

fil_liste

fil_liste = glob.glob('/ssb/stamme01/fylkhels/speshelse/felles/brukere/2024/111224/prod/*')

for filsti in fil_liste:
    dfs.append(pd.read_csv(filsti, encoding='latin1', sep=';'))

dfs[4]


