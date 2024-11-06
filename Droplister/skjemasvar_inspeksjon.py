# - Sjekke tidligere skjemasvar.
#     - Har enheten levert tidligere?
#     - Står det noe i merknadsfeltet?
# - Har RHFene vist til enheten på sine nettsider?
#     - Hvis ikke, skal den ut.
#     - Hvis de ikke tilbyr døgnbehandling, men har avtale med RHF, skal de ligge i delreg 24XX for å få med personelltallene
# - Registrere i delregisteret at de ikke skal ha skjema 47 (men de skal være med i populasjonen pga personelltall)
#     - Slett innhold i alle hjelpefelter
#         - Rapporteringsnr: settes til skjemanummer (47)
#     - Velg Skjema Type nederst og trykk fjern post-knappen
#     - Lagre
# - Hvis enheten skal tas ut av delregisteret (har ingen avtale)
#     - Sett Kvitteringstype til "Avgang"
#     - Slett også alle hjelpefelt
#     - Slett Skjema Type nederst (fjern post-knapp)
# - Eventeuelle nye enheter legges til til slutt i delregisteret

data = """Avonova Ringerike Rehabilitering AS	SOROST
Beitostølen Helsesportsenter AS	SOROST
Evjeklinikken AS	SOROST
Falck Norge AS	SOROST
Godthaab Helse og Rehabilitering AS	SOROST
HLF Rehabilitering as	SOROST
Høyenhall Helse og Rehabilitering as	SOROST
Idrettens Helsesenter AS	SOROST
Lovisenberg Rehabilitering AS	SOROST
MS-Senteret Hakadal AS	SOROST
N.K.S Helsehus Akershus as	SOROST
Norsk Diabetessenter AS	SOROST
PTØ Gardermoen	SOROST
Rehabiliteringssenteret AiR	SOROST
Ringen Rehabiliteringssenter AS	SOROST
Røysumtunet	SOROST
Signo Conrad Svendsen senter	SOROST
Skogli Helse- og rehabiliteringssenter AS	SOROST
Stiftelsen Catosenteret AS	SOROST
Stiftelsen Hernes Institutt	SOROST
Sørlandets rehabiliteringssenter Eiken AS	SOROST
Unicare Bakke AS	SOROST
Unicare Fram AS	SOROST
Unicare Friskvernklinikken AS	SOROST
Unicare Hokksund AS	SOROST
Unicare Jeløy AS	SOROST
Unicare Landaasen AS	SOROST
Unicare Steffensrud AS	SOROST
Vikersund Bad Rehabiliteringssenter AS	SOROST
Namdal Rehabilitering IKS	MIDT
We Care Trondheim	MIDT
LHL-klinikkene Trondheim	MIDT
Unicare Røros	MIDT
Selli Rehabiliteringssenter	MIDT
Muritunet	MIDT
Meråker kurbad	MIDT
Kastvollen Rehabiliteringssenter	MIDT
Treningsklinikken	MIDT
Unicare Coperio Trondheim	MIDT
Unicare helsefort	MIDT
Falck	MIDT
Betania Malvik	MIDT
Friskgården	MIDT
Røde Kors Haugland Rehabiliteringssenter i Fjaler	VEST
Åstveit Helsesenter i Bergen	VEST
Barnas Fysioterapisenter i Bergen	VEST
Ravneberghaugen, Senter for meistring og rehabilitering i Hagavik	VEST
Rehabilitering Vest i Haugesund (1. mai 2024)	VEST
Falck Norge i Randaberg	VEST
PTØ-senteret i Stavanger	VEST
Nærland Rehabilitering på Jæren 	VEST
Helsepartner Rehabilitering, avd. Alta	NORD
Helsepartner Rehabilitering, avd. Skibotn 	NORD
ViGØR Rehabiliteringssykehus 	NORD
Valnesfjord Helsesportssenter	NORD
Nordtun HelseRehab	NORD
Andre avtaler	NORD
Stiftelsen RIBO	NORD
Røysumtunet	NORD
MS-senteret Hakadal	NORD
Lovisenberg Rehabilitering	NORD
"""

datalist = data.split("\n")

import pandas as pd

import difflib

df = pd.DataFrame([x.split("\t") for x in datalist], columns = ['NAVN_RHF', 'REGION'])

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('2424')
"""
SFU_data = hjfunk.les_sql(sporring, conn)
print(f"Rader:    {SFU_data.shape[0]}\nKolonner: {SFU_data.shape[1]}")
SFU_data.info()

SFU_data["NAVN_FULL"] = (
    SFU_data["NAVN1"].fillna("")
    + " "
    + SFU_data["NAVN2"].fillna("")
    + " "
    + SFU_data["NAVN3"].fillna("")
    + " "
    + SFU_data["NAVN4"].fillna("")
    + " "
    + SFU_data["NAVN5"].fillna("")
)

SFU_kols = (['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE', 'SKJEMA_TYPE']
         + [x for x in SFU_data.columns if "ORGNR" in x]
         + ['NAVN_FULL']
         + [x for x in SFU_data.columns if "H_VAR" in x])
SFU_data[SFU_kols]

SFU47 = SFU_data[SFU_data['SKJEMA_TYPE'].fillna("").str.contains("47")][SFU_kols]

SFU47



dropliste_sti = "/ssb/stamme01/fylkhels/speshelse/felles/droplister/2024/291024/Dropliste_skj47_2024_291024.csv"

dropliste = pd.read_csv(dropliste_sti, sep=";", encoding='latin1')

# +
import pandas as pd
from difflib import SequenceMatcher

def match_names(
    df1, df2,
    df1_name_col,
    df2_name_col,
    df1_additional_cols=None,
    df2_additional_cols=None,
    score_threshold=0,
    preprocess=True
):
    """
    Matcher navn fra df1 til df2, beregner matchingsscore,
    og kartlegger tilbake til originale navn og tilleggsinformasjon.

    Parametere:
    - df1: pd.DataFrame
        Første DataFrame som inneholder navnene som skal matches.
    - df2: pd.DataFrame
        Andre DataFrame som inneholder referansenavnene å matche mot.
    - df1_name_col: str eller liste av str
        Kolonnenavn i df1 for navnene som skal matches. Kan være en streng eller liste av strenger.
    - df2_name_col: str
        Kolonnenavn i df2 som inneholder navnene å matche mot.
    - df1_additional_cols: liste av str eller None
        Valgfrie kolonner i df1 som skal beholdes i resultatet.
    - df2_additional_cols: liste av str eller None
        Valgfrie kolonner i df2 som skal legges til resultatet basert på matchene.
    - score_threshold: float
        Minimum score for å vurdere en match (mellom 0 og 1).
    - preprocess: bool
        Om navnene skal forhåndsbehandles (små bokstaver og fjerne mellomrom).

    Returnerer:
    - result_df: pd.DataFrame
        DataFrame som inneholder df1s data, matchingsresultater, scorer, og valgfri tilleggsinformasjon fra df2.
    """

    # Lag kopier for å unngå å endre originaldataene
    df1 = df1.copy()
    df2 = df2.copy()

    # Håndter tilfelle der df1_name_col er en enkelt streng
    if isinstance(df1_name_col, str):
        df1_name_cols = [df1_name_col]
    else:
        df1_name_cols = df1_name_col

    # Forhåndsbehandle navn hvis ønsket
    if preprocess:
        for col in df1_name_cols:
            df1[col + '_clean'] = df1[col].astype(str).str.lower().str.strip()
        df2['df2_name_clean'] = df2[df2_name_col].astype(str).str.lower().str.strip()
    else:
        for col in df1_name_cols:
            df1[col + '_clean'] = df1[col]
        df2['df2_name_clean'] = df2[df2_name_col]

    # Kombiner navnekolonner i df1 til en enkelt streng for matching
    df1['combined_name_clean'] = df1[[col + '_clean' for col in df1_name_cols]].fillna('').agg(' '.join, axis=1).str.strip()

    # Opprett liste over unike navn i df2 for matching
    df2_names = df2['df2_name_clean'].dropna().unique()

    # Funksjon for å finne beste match og score
    def get_best_match_and_score(name, choices):
        if not name:
            return pd.Series([None, None])
        best_match = None
        highest_score = 0
        for choice in choices:
            score = SequenceMatcher(None, name, choice).ratio()
            if score > highest_score:
                highest_score = score
                best_match = choice
        if highest_score >= score_threshold:
            return pd.Series([best_match, highest_score])
        else:
            return pd.Series([None, None])

    # Anvend matching på df1
    df1[['match_name_clean', 'match_score']] = df1['combined_name_clean'].apply(
        lambda x: get_best_match_and_score(x, df2_names)
    )

    # Kartlegg tilbake til originale navn og tilleggsinformasjon
    df2_mapping_cols = [df2_name_col] + (df2_additional_cols if df2_additional_cols else [])
    df2_mapping = df2.set_index('df2_name_clean')[df2_mapping_cols].drop_duplicates()

    # Slå sammen df1 med df2_mapping basert på match_name_clean
    result_df = df1.merge(df2_mapping, how='left', left_on='match_name_clean', right_index=True)

    # Velg kolonner å inkludere i resultatet
    result_cols = df1_name_cols + ['match_score'] + df2_mapping_cols
    if df1_additional_cols:
        result_cols += df1_additional_cols

    return result_df[result_cols]



# -

result_df = match_names(
    df1=SFU47,
    df2=df,
    df1_name_col=['NAVN_FULL'],  # Kolonner i df1 som inneholder navn å matche
    df2_name_col='NAVN_RHF',      # Kolonne i df2 å matche mot
    df1_additional_cols=['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE', 'SKJEMA_TYPE', 'ORGNR', 'ORGNR_FORETAK'],  # Behold denne kolonnen fra df1
    df2_additional_cols=['REGION'],  # Ta med disse kolonnene fra df2
    score_threshold=0.2,
    preprocess=True
)

result_df.sort_values('match_score')



# +
import pandas as pd
from difflib import SequenceMatcher

# Forhåndsbehandle navnene
dropliste['FORETAK_NAVN_clean'] = dropliste['FORETAK_NAVN'].str.lower().str.strip()
dropliste['FINST_NAVN_clean'] = dropliste['FINST_NAVN'].str.lower().str.strip()
df['NAVN_clean'] = df['NAVN'].str.lower().str.strip()

# Funksjon for å finne beste match og score
def get_best_match_and_score(name, choices):
    best_match = None
    highest_score = 0
    for choice in choices:
        score = SequenceMatcher(None, name, choice).ratio()
        if score > highest_score:
            highest_score = score
            best_match = choice
    return pd.Series([best_match, highest_score])

# Finn beste matcher og scorer
dropliste[['FORETAK_NAVN_MATCH_clean', 'FORETAK_NAVN_SCORE']] = dropliste['FORETAK_NAVN_clean'].apply(
    lambda x: get_best_match_and_score(x, df['NAVN_clean'])
)

dropliste[['FINST_NAVN_MATCH_clean', 'FINST_NAVN_SCORE']] = dropliste['FINST_NAVN_clean'].apply(
    lambda x: get_best_match_and_score(x, df['NAVN_clean'])
)

# Lag mappings
navn_dict = df.set_index('NAVN_clean')['NAVN'].to_dict()
region_dict = df.set_index('NAVN_clean')['REGION'].to_dict()

# Kartlegg tilbake til originale navn og legg til REGION
dropliste['FORETAK_NAVN_MATCH'] = dropliste['FORETAK_NAVN_MATCH_clean'].map(navn_dict)
dropliste['FORETAK_NAVN_REGION'] = dropliste['FORETAK_NAVN_MATCH_clean'].map(region_dict)
dropliste['FINST_NAVN_MATCH'] = dropliste['FINST_NAVN_MATCH_clean'].map(navn_dict)
dropliste['FINST_NAVN_REGION'] = dropliste['FINST_NAVN_MATCH_clean'].map(region_dict)

# +
m_score1 = dropliste['FINST_NAVN_SCORE'] < 1
m_score2 = dropliste['FORETAK_NAVN_SCORE'] < 1

m_na = dropliste['D_HELD_IFJOR'].isna() | dropliste['SDGN_HT_FJOR'].isna()

m_mangler_data_og_har_ikke_100prs_treff = m_score1 & m_score2 & m_na
# -

dropliste.columns

kolonner = ['FORETAK_ORGNR', 'FINST_ORGNR', 'FORETAK_NAVN', 'FORETAK_NAVN_MATCH', 'FORETAK_NAVN_SCORE', 'FORETAK_NAVN_REGION', 'FINST_NAVN', 'FINST_NAVN_MATCH', 'FINST_NAVN_SCORE', 'FINST_NAVN_REGION', 'SDGN_HT_FJOR', 'D_HELD_IFJOR']

dropliste[m_mangler_data_og_har_ikke_100prs_treff][kolonner].sort_values(['FINST_NAVN_SCORE', 'FORETAK_NAVN_SCORE'])

import hjelpefunksjoner as hjfunk
from sqlalchemy import create_engine
import getpass

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

# +
årganger = ['23', '22', '21']

skjemadata = {}
for år in årganger:
    skjemadata[år] = hjfunk.hent_data_delreg24x_og_19377x(
        år, "HELSE47", conn
    )[['FINST_NAVN', 'FINST_ORGNR', 'FORETAKETS_NAVN', 'FORETAKETS_ORGNR', 'SDGN_SUM', 'D_HELD', 'MERKNAD1', 'MERKNAD0']]
# -

pd.options.display.max_colwidth = 200

skjemadata['23'].columns

skjemadata['23']



dropliste[dropliste['FORETAK_ORGNR'] == 893140832]


