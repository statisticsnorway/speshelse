# +
import pandas as pd
import cx_Oracle
from db1p import query_db1p
import getpass
import datetime as dt

til_lagring = True # Sett til True, hvis du skal gjøre endringer i Databasen
# -

import warnings
warnings.filterwarnings('ignore')

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

# +
år4 = 2022
år2 = str(år4)[-2:]

år_før4 = år4 - 1            # året før
år_før2 = str(år_før4)[-2:]
# -

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# ## Andre purrerunde
#
# Ideen er at de rette kontaktpersonene muligens ikke har fått beskjed via Altinn. Vi kan dermed se på epost-adressen som leverte tallene året før hvis det er mulig. Dette scriptet henter en oversikt over alle som leverte året før.
#
# Man trenger lesetilgang til KOSTRA_EXP@DB1P for aktuelle årganger. Her har spesialisthelsetjenesten en tabell per skjema per årgang. Tabellene er navngitt ‘HELSE’ + skjemanummer + ‘_’ + årgang som gir f.eks `HELSE0X_2021`. 
#

sporring = """
SELECT *
FROM all_tables

ORDER BY table_name ASC
"""
tabeller = pd.read_sql_query(sporring, conn)
print(f"Rader:    {tabeller.shape[0]}\nKolonner: {tabeller.shape[1]}")

tabellnavn = (
    tabeller
    [tabeller['TABLESPACE_NAME'] == "DSBBASE"]
    .TABLE_NAME.unique()
)

mask_kostra = tabeller['OWNER'] == "KOSTRA_EXP"
mask_helse = tabeller['TABLE_NAME'].str.startswith("HELSE")
mask_årgang = tabeller['TABLE_NAME'].str.endswith(str(år_før4))
skjemaer = tabeller[mask_kostra & mask_helse & mask_årgang].TABLE_NAME.unique()

utkols_o = ["FORETAKETS_ORGNR", "FORETAKETS_NAVN", "HELSEREGION_UTFYLT",
            "HELSEREGION_EPOST", "SKJEMA_TYPE", "AARGANG"]

# Mangler FORETAKETS_NAVN i registeret (Vurdere å kople på). Setter inn 
# tom variabel KVARTAL og sletter denne senere
utkols_0X0Y = ["ORG_NR", "KVARTAL", "SKJEMA_UTFYLT",
               "EPOSTADR", "SKJEMA_TYPE",
               "AARGANG"]

utkols_48 = ['FORETAKETS_ORGNR',
             'FORETAKETS_NAVN',
             'HELSEREGION_KONTAKTPERSON',
             'HELSEREGION_EPOST',
             'SKJEMA_TYPE',
             'AARGANG']

utkols_46P = ['FORETAK_ORGNR',
             'FORETAK_NAVN',
             'HELSEREGION_UTFYLT',
             'HELSEREGION_EPOST',
             'SKJEMA_TYPE',
             'AARGANG']

kontakt_df = pd.DataFrame()


def hent_skjema(s):
    sporring = f"""
    SELECT *
    FROM KOSTRA_EXP.{s}
    """
    df = pd.read_sql_query(sporring, conn)
    return df


skj_dict = {}
for skj in skjemaer:
    df = hent_skjema(skj)
    s = skj.replace("_" + str(år_før4),"")
    skj_dict[s] = df

skjemaer_med_utkols_o = []
for skj in skj_dict:
    alle_true = True
    for kol in utkols_o:
        er_inne = kol in skj_dict[skj].columns
        if not er_inne:
            alle_true = False
    if alle_true:
        skjemaer_med_utkols_o.append(skj)

skjemaer_med_andre_kol_navn = list(set(skj_dict.keys())-set(skjemaer_med_utkols_o))

skj_kol_dict = {
    'HELSE0X': utkols_0X0Y,
    'HELSE0Y': utkols_0X0Y,
    'HELSE38O': utkols_o,
    'HELSE38P': utkols_o,
    'HELSE39': utkols_o,
    'HELSE40': utkols_o,
    'HELSE41': utkols_o,
    'HELSE44O': utkols_o,
    'HELSE44P': utkols_o,
    'HELSE45O': utkols_o,
    'HELSE45P': utkols_o,
    'HELSE46O': utkols_o,
    'HELSE46P': utkols_46P,
    'HELSE46': utkols_o,
    'HELSE47': utkols_o,
    'HELSE48': utkols_48,
}

kontakt_df = pd.DataFrame()

for skj in skj_dict:
    kolonner_temp = skj_kol_dict[skj]
    df_temp = skj_dict[skj][kolonner_temp].copy()
    
    # Har nå forsikret meg om at kolonnerekkefølgen er riktig og gjør endring basert på rekkefølge
    df_temp.columns = ['ORGNR_FORETAK', 'NAVN_FORETAK', 'KONTAKTPERSON', 'EPOSTADR', 'SKJEMA_TYPE', 'AARGANG']
    
    kontakt_df = pd.concat([kontakt_df, df_temp])


kontakt_df = kontakt_df.drop_duplicates()

kontakt_df.sample(10)


