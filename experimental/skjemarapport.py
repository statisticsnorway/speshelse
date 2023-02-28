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
aar4 = 2022
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

# # Skjemaoversikt
# Hvordan ligger skjemainnleveringen an?

# ## SFU: `dsbbase.dlr_enhet_i_delreg `
# På enhetsnivå

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
""" 
SFU_enhet = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_enhet.shape[0]}\nKolonner: {SFU_enhet.shape[1]}")


# Enheter med verdi i 'KVITT_TYPE' filtreres ut (de er nedlagte enheter). Fjerner også enheter uten ORGNR
SFU_enhet = SFU_enhet[SFU_enhet['ORGNR'].notnull()]

# # Skjema-SFU: `dsbbase.dlr_enhet_i_delreg_skjema `

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_skjema = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_skjema.shape[0]}\nKolonner: {SFU_skjema.shape[1]}")


# # Kobler SFU-enhet og SFU-skjema

SFU = pd.merge(
    SFU_skjema,
    SFU_enhet,
    how='left',
    on='IDENT_NR',
    suffixes=("_skj","_enh")
)

# Alle skjema i SFU med antall foremkomster på virksomhetsnivå:

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

import numpy as np
import pandas as pd   
from IPython.display import display, HTML
import matplotlib.pyplot as plt
CSS = """
.output {
    flex-direction: row;
}
"""
display(HTML('<style>{}</style>'.format(CSS)))

skjemaoversikt = pd.DataFrame([tot, ant_ikke_levert]).T
skjemaoversikt.columns = ['tot', 'ikke_levert']
skjemaoversikt['levert'] = skjemaoversikt['tot'] - skjemaoversikt['ikke_levert']
skjemaoversikt['prosentandel_levert'] = round(skjemaoversikt['levert'] / skjemaoversikt['tot'] * 100)
skjemaoversikt = skjemaoversikt.sort_values('prosentandel_levert', ascending=False)
display(skjemaoversikt)
skjemaoversikt.prosentandel_levert.plot(kind='bar',
                                        grid=True,
                                        ylim=(0,100),
                                        title="Prosentvis andel virksomheter som har levert:"
                                        );



