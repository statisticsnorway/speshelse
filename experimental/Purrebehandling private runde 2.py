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


def print_size(df):
    print(f"Rader:    {df.shape[0]}\nKolonner: {df.shape[1]}")


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
print_size(tabeller)

tabellnavn = (
    tabeller
    [tabeller['TABLESPACE_NAME'] == "DSBBASE"]
    .TABLE_NAME.unique()
)

# `tabeller` inneholder alle tabellene i Oracle. Den er som et oppslagsverk for å finne hvor hvordan man skrive SQL-spørringer. Filtrerer ut tabellene i tråd med navnsystemet beskrevet over.

mask_kostra = tabeller['OWNER'] == "KOSTRA_EXP"
mask_helse = tabeller['TABLE_NAME'].str.startswith("HELSE")
mask_årgang = tabeller['TABLE_NAME'].str.endswith(str(år_før4))
skjemaer = tabeller[mask_kostra & mask_helse & mask_årgang].TABLE_NAME.unique()

# Velger ut de kolonnene jeg ønsker å ha med videre, men ser at kolonnenavnene ikke er likt på tvers av skjemaene. Har dermed manuelt gått gjennom tabellene og sett hva de heter. Legger til slutt en dictionary med oversikt over hvilke skjemaer som har hvilke kolonnenavn.

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

skj_kol_dict = {
    'HELSE0X':  utkols_0X0Y,
    'HELSE0Y':  utkols_0X0Y,
    'HELSE38O': utkols_o,
    'HELSE38P': utkols_o,
    'HELSE39':  utkols_o,
    'HELSE40':  utkols_o,
    'HELSE41':  utkols_o,
    'HELSE44O': utkols_o,
    'HELSE44P': utkols_o,
    'HELSE45O': utkols_o,
    'HELSE45P': utkols_o,
    'HELSE46O': utkols_o,
    'HELSE46P': utkols_46P,
    'HELSE46':  utkols_o,
    'HELSE47':  utkols_o,
    'HELSE48':  utkols_48,
}


def hent_skjema(s):
    """
    Funksjon som henter inn alle variable fra angitt tabell s.
    """

    sporring = f"""
    SELECT *
    FROM KOSTRA_EXP.{s}
    """
    df = pd.read_sql_query(sporring, conn)
    return df


# tar bort årgang fra tabellnavnet
skj_dict = {}
for skj in skjemaer:
    df = hent_skjema(skj)
    s = skj.replace("_" + str(år_før4), "")
    skj_dict[s] = df

# lager en tom dataframe setter sammen til en stor mastertabell
kontakt_df = pd.DataFrame()

for skj in skj_dict:
    kolonner_temp = skj_kol_dict[skj]
    df_temp = skj_dict[skj][kolonner_temp].copy()

    if skj == "HELSE0X" or skj == "HELSE0Y":
        # brukte denne variabelen som dummy-variabel, fordi navn på foretak ikke
        # var med i denne tabellen
        df_temp['KVARTAL'] = ""

    # Har nå forsikret meg om at kolonnerekkefølgen er lik for alle tabeller
    # og gjør navneendring basert på rekkefølge
    df_temp.columns = ['ORGNR_FORETAK', 'NAVN_FORETAK', 'KONTAKTPERSON', 'EPOSTADR', 'SKJEMA_TYPE', 'AARGANG']

    kontakt_df = pd.concat([kontakt_df, df_temp])


kontakt_df = kontakt_df.drop_duplicates()

print_size(kontakt_df)
display(kontakt_df.sample(3))

# # Vanlig SFU: `dsbbase.dlr_enhet_i_delreg `
# På enhetsnivå

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{år2}')
""" 
SFU_enhet = pd.read_sql_query(sporring, conn)
print_size(SFU_enhet)


# Fjerner enheter uten ORGNR
SFU_enhet = SFU_enhet[SFU_enhet['ORGNR'].notnull()]

# # Skjema-SFU: `dsbbase.dlr_enhet_i_delreg_skjema `

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
    WHERE DELREG_NR IN ('24{år2}')
"""
SFU_skjema = pd.read_sql_query(sporring, conn)
print_size(SFU_skjema)


SFU = pd.merge(
    SFU_skjema,
    SFU_enhet,
    how='left',
    on='IDENT_NR',
    suffixes=("_skj","_enh")
)

# ## Setter sammen en tabell med kontaktinformasjon på alle som ikke har levert

visningskolonner = ['ORGNR_FORETAK', 'ORGNR', 'NAVN',
                    'SKJEMA_TYPE_skj', 'E_POST']

ikke_levert_mask = SFU['KVITT_TYPE_skj'].isnull()
ikke_skjema39_eller_ambu = ~SFU['SKJEMA_TYPE_skj'].isin(["HELSE39", "RA-0595"])

# +
purre_df = SFU[ikke_levert_mask & ikke_skjema39_eller_ambu][visningskolonner].copy()
purre_df = purre_df.rename(
    columns={'E_POST': 'OFF_EPOST',
             'SKJEMA_TYPE_skj': 'SKJEMA_TYPE'
             }
)

purre_df = (
    pd.merge(
        purre_df,
        kontakt_df,
        how='left',
        on='ORGNR_FORETAK',
        suffixes=("_purre", "_kontakt")
    )
)
purre_df.sample(3)
# -

print("Skjematyper ikke alt er levert i:")
print(list(purre_df.SKJEMA_TYPE_purre.unique()))


def lag_epostliste(liste_med_skjema):

    temp_df = purre_df[purre_df['SKJEMA_TYPE_purre'].isin(liste_med_skjema)]

    ingen_info = list(purre_df[purre_df.KONTAKTPERSON.isna()].NAVN.unique())
    epostliste = list(temp_df.EPOSTADR.dropna().unique())
    epostlistestr = '; '.join(map(str, epostliste))
    print("Disse har vi ingen tidligere kontaktinformasjon fra: ", ingen_info)

    print()

    print("Her er epostliste med kontaktinformasjon fra skjema ", liste_med_skjema)
    print(epostlistestr)


lag_epostliste(['HELSE0X'])



