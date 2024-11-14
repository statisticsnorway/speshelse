# # Sammenlikn enhetsresultater og -navn

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
#     - Sett Kvitteringstype til "Avgang" og "Feilaktig i utvalget"
#     - Slett også alle hjelpefelt
#     - Slett Skjema Type nederst (fjern post-knapp)
# - Eventeuelle nye enheter legges til til slutt i delregisteret

# +
import pandas as pd
import difflib
import glob
import getpass
from sqlalchemy import create_engine

import hjelpefunksjoner as hjfunk
# -

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

# ## Private rehabiliteringsenheter med RHF-avtale

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
Rehabilitering Vest i Haugesund	VEST
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

df = pd.DataFrame([x.split("\t") for x in datalist], columns = ['NAVN_RHF', 'REGION'])

df.head(3)

# ## Masterfilanalyse fra fjorårets datainnsamling
# Brukes til å fange opp enheter som ikke har rapportert oppholdsdøgn på foretaksnivå.

# +
# Finn siste fil manuelt ved å bytte ut årstallet
sti_an = "/ssb/stamme01/fylkhels/speshelse/felles/populasjon/2023/**"

glob.glob(sti_an)
# -

an_df = pd.read_excel('/ssb/stamme01/fylkhels/speshelse/felles/populasjon/2023/populasjonsanalyse_2023_2024-06-06-1050.xlsx')

an_reh = an_df[(an_df['TJENESTE_KODE'] == 'REH')]

an_reh.head(3)

# ## Populasjon SFU delreg 24XX

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
         + ['NAVN_FULL'])
SFU_data[SFU_kols].sample(3)

SFU47 = SFU_data[SFU_data['SKJEMA_TYPE'].fillna("").str.contains("47")][SFU_kols]

SFU47.head(3)



# ### Merge SFU skjema 47 med analyserapporten

sjekk = pd.merge(
    SFU47,
    an_reh,
    how='outer',
    left_on='ORGNR_FORETAK',
    right_on='ORGNR_FRTK',
    suffixes=('_SFU', '_ANALYSE'),
    indicator=True
)

sjekk[sjekk['OPPHOLDSDOGN'] == 0].head(3)

# ## Dropliste

glob.glob("/ssb/stamme01/fylkhels/speshelse/felles/droplister/2024/**")

# Bruk glob til å finne siste filsti
dropliste_sti = '/ssb/stamme01/fylkhels/speshelse/felles/droplister/2024/061124/Dropliste_skj47_2024_061124.csv'

dropliste = pd.read_csv(dropliste_sti, sep=";", encoding='latin1')

# ## Sammenlikn SFU-navn i listen av enheter med RHF-avtale 

# +
import re
from difflib import SequenceMatcher

def match_names(
    df1, df2,
    df1_name_col,
    df2_name_col,
    df1_additional_cols=None,
    df2_additional_cols=None,
    exclude_words=None,
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
    - exclude_words: liste av str eller None
        Liste over ord som skal fjernes fra navnene under forhåndsbehandling.
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
        # Lag regex-mønster for å fjerne ekskluderte ord
        if exclude_words:
            # Escape spesialtegn i exclude_words
            exclude_pattern = r'\b(' + '|'.join(map(re.escape, exclude_words)) + r')\b'
        else:
            exclude_pattern = None

        for col in df1_name_cols:
            clean_col = df1[col].astype(str).str.lower().str.strip()
            if exclude_pattern:
                clean_col = clean_col.str.replace(exclude_pattern, '', regex=True)
            df1[col + '_clean'] = clean_col.str.replace(r'\s+', ' ', regex=True).str.strip()

        clean_col = df2[df2_name_col].astype(str).str.lower().str.strip()
        if exclude_pattern:
            clean_col = clean_col.str.replace(exclude_pattern, '', regex=True)
        df2['df2_name_clean'] = clean_col.str.replace(r'\s+', ' ', regex=True).str.strip()
    else:
        for col in df1_name_cols:
            df1[col + '_clean'] = df1[col]
        df2['df2_name_clean'] = df2[df2_name_col]

    # Kombiner navnekolonner i df1 til en enkelt streng for matching
    df1['combined_name_clean'] = df1[[col + '_clean' for col in df1_name_cols]].fillna('').agg(' '.join, axis=1).str.strip()

    # Fjern doble mellomrom
    df1['combined_name_clean'] = df1['combined_name_clean'].str.replace(r'\s+', ' ', regex=True)
    df2['df2_name_clean'] = df2['df2_name_clean'].str.replace(r'\s+', ' ', regex=True)

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
    exclude_words=['as', 'rehabilitering', 'avd'],
    score_threshold=0,
    preprocess=True
)

# ### Sjekk enheter uten 100% match på navn

result_df[result_df['match_score'] <= 1].sort_values('match_score')



# ## Søk etter tekst i to navnekolonner

def search_text(df, col, search_text) -> None:
    stu = search_text.upper()
    res = df.loc[df[col].fillna("").str.upper().str.contains(stu)]
    
    res = res[[col] + [x for x in res.columns if x != col]]
    display(res)


search_text(df, 'NAVN_RHF', 'NÆRLAND')

search_text(SFU47, 'NAVN_FULL', 'nærland')



result_df2 = match_names(
    df1=dropliste,
    df2=df,
    df1_name_col=['FINST_NAVN'],  # Kolonner i df1 som inneholder navn å matche
    df2_name_col='NAVN_RHF',      # Kolonne i df2 å matche mot
    df1_additional_cols=['FORETAK_ORGNR', 'FINST_ORGNR', 'D_HELD_IFJOR', 'SDGN_HT_FJOR'],  # Behold kolonner fra df1
    df2_additional_cols=['REGION'],  # Ta med disse kolonnene fra df2
    exclude_words=['as', 'rehabilitering', 'avd'],
    score_threshold=0,
    preprocess=True
)

result_df2.sort_values('match_score')

# ## Skjema 47-svar fra siste årganger

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


