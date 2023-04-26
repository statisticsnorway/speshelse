import pandas as pd
import cx_Oracle
import getpass

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# +
sporring = """
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('2422')
"""

tabell = pd.read_sql_query(sporring, conn)
# -

tabell.info(max_cols=500)

sporring = """
    SELECT *
    FROM all_tables
"""
at = pd.read_sql_query(sporring, conn)

at = at[at['OWNER']=='DSBBASE']

navn = at['TABLE_NAME'].to_list().copy()

navn.sort()

navn

# +
sporring = """
    SELECT *
    FROM DSBBASE.DLR_BRUKER_PW
"""

df = pd.read_sql_query(sporring, conn)
df
# -

df
