# -*- coding: utf-8 -*-
# # Brukerlister
# Dette er en oversikt over alle foretak som skal ha skjemaer og hvilke skjemaer de skal få. Også kalt _populasjonsliste_ av KOSTRA-IT.
#
# - Alle hjelpeforetak som skal rapportere skal ha skjema: `"0X 0Y 40"`
#     - Dette ser ut til å være alle som ikke er apotek. Foretaksnummerne til disse er listet opp under.
# - Alle RHF skal ha skjema: `"0X 0Y 40 41"`
# - Alle HF skal ha skjema: `"0X 0Y 40 380 440 450 460 48"`
# - Alle foretak med oppdrag og bestillerdokument skal ha skjema: `"39 381 441 451 461 47 48"`
# - Alle private foretak skal ha skjema: `"39 381 441 451 461 47"`

til_lagring = False

aar4 = 2023
aar2 = str(aar4)[-2:]
aarfør4 = aar4-1
aarfør2 = str(aarfør4)[-2:]

# +
import pandas as pd
from klass import get_classification
import cx_Oracle
import getpass


# Fjerner begrensning på antall rader og kolonner som vises av gangen
pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

# Unngå standardform i output
pd.set_option('display.float_format', lambda x: '%.0f' % x)
# -

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# ## Hjelpeforetak

klass_rfss = get_classification(605).get_codes()

rfss2 = (
    get_classification(605)
    .get_codes()
    .data.query("level == '2' & parentCode == '99'")
    [['code', 'name']]
    .rename(columns={
        'code': 'ORGNR_FORETAK',
        'name': 'FORETAK_NAVN'
        }
    )
)

rfss3 = (
    get_classification(605)
    .get_codes()
    .data.query("level == '2' & parentCode != '99'")
    [['code', 'name']]
    .rename(columns={
        'code': 'ORGNR_FORETAK',
        'name': 'FORETAK_NAVN'
        }
    )
)

RFSS = pd.concat([rfss2, rfss3])

RFSS

# +
RFSS['SKJEMA_TYPE'] = "0X 0Y 40"

print("Antall: ", RFSS.shape[0])
RFSS.sample(3)
# -

# ## RHF

klass_offentlige_helseforetak = get_classification(603).get_codes()

RHF = klass_offentlige_helseforetak.pivot_level()
RHF = (
    RHF[['name_2', 'code_2']]
    .drop_duplicates()
    .rename(columns={'name_2': 'FORETAK_NAVN',
                     'code_2': 'ORGNR_FORETAK'})
)

RHF['SKJEMA_TYPE'] = "0X 0Y 40 41 48"

print("Antall: ", RHF.shape[0])
RHF.sample(3)

# ## HF

HF = klass_offentlige_helseforetak.pivot_level()
HF = (
    HF.rename(columns={
        'code_3': 'ORGNR_FORETAK',
        'name_3': 'FORETAK_NAVN'
    }
             )
    [['ORGNR_FORETAK', 'FORETAK_NAVN']]
)

HF['SKJEMA_TYPE'] = "0X 0Y 40 380 440 450 460 48"
print("Antall: ", HF.shape[0])
HF.sample(3)


# ## Private

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
## Henter ORGNR_FORETAK fra 24xx
sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_data = pd.read_sql_query(sporring, conn)
print(f"Fra '24{aar2}'\nRader:    {SFU_data.shape[0]}\nKolonner: {SFU_data.shape[1]}")

foretak = SFU_data[SFU_data['H_VAR2_A'] == 'PRIVAT'][['ORGNR_FORETAK']].to_numpy()

sql_str = lag_sql_str(foretak)

## Henter navn fra SSB_FORETAK-databasen:
sporring_for = f"""
    SELECT *
    FROM DSBBASE.SSB_FORETAK
    WHERE STATUSKODE = 'B' AND ORGNR IN {sql_str}
"""

PRIV = pd.read_sql_query(sporring_for, conn)[['NAVN', 'ORGNR']]
PRIV = PRIV.rename(columns={'NAVN': 'FORETAK_NAVN',
                            'ORGNR': 'ORGNR_FORETAK'})
PRIV['SKJEMA_TYPE'] = "39 381 441 451 461 47"

print("\nFra DSBBASE.SSB_FORETAK\nAntall foretak: ", PRIV.shape[0])
PRIV.sample(3)
# -

# ## Oppdrags- og bestillerdokument

# +
klass_priv_frtk_ob = get_classification(604).get_codes()

PHOB = klass_priv_frtk_ob.pivot_level()

PHOB = (
    PHOB.rename(columns={
        'code_2': 'ORGNR_FORETAK',
        'name_2': 'FORETAK_NAVN'
    }
             )
    [['ORGNR_FORETAK', 'FORETAK_NAVN']]
)

# +
PHOB['SKJEMA_TYPE'] = "39 381 441 451 461 47 48"

print("Antall: ", PHOB.shape[0])
PHOB.sample(3)
# -

# ## Slå sammen til en dataframe som eksporteres til `.csv`

# +
brukerliste_dfs = [RFSS, RHF, HF, PRIV, PHOB]
brukerliste_df = pd.concat(brukerliste_dfs)
print("Antall foretak i brukerlisten: ", brukerliste_df.shape[0])


brukerliste_df['FORETAK_NAVN'] = brukerliste_df['FORETAK_NAVN'].str.upper()

brukerliste_df = brukerliste_df.reset_index(drop=True)
# -


brukerliste_df.sample(3)

# +
dato_idag = pd.Timestamp("today").strftime("%d%m%y")
filnavn = "Brukerliste" + "_" + str(aar4) + "_" + dato_idag + ".csv"
sti_til_lagring = "/ssb/stamme01/fylkhels/speshelse/felles/droplister/" + str(aar4) + "/" + str(dato_idag) + "/"

if til_lagring:
    brukerliste_df.to_csv(
                      sti_til_lagring + filnavn, 
                      sep=";", 
                      encoding='latin1', 
                      index=False
                     )
    print(f"Filen er lagret på {sti_til_lagring + filnavn}")
# -


