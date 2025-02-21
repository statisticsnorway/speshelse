# # Lese fra og skrive til delregister i Oracle

# +
import pandas as pd
import cx_Oracle
from db1p import query_db1p
import getpass
import datetime as dt

til_lagring = True # Sett til True, hvis du skal gjøre endringer i Databasen
# -

pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

conn = cx_Oracle.connect(getpass.getuser()+"/"+getpass.getpass(prompt='Oracle-passord: ')+"@DB1P")

# +
aar4 = 2024
aar2 = str(aar4)[-2:]

aar_før4 = aar4 - 1            # året før
aar_før2 = str(aar_før4)[-2:]
# -

# # Vanlig SFU: `dsbbase.dlr_enhet_i_delreg `
# På enhetsnivå

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('24{aar2}')
""" 
SFU_enhet = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_enhet.shape[0]}\nKolonner: {SFU_enhet.shape[1]}")


# Fjerner enheter uten ORGNR
print(SFU_enhet['ORGNR'].isna().sum())
SFU_enhet = SFU_enhet[SFU_enhet['ORGNR'].notnull()]
print(SFU_enhet['ORGNR'].isna().sum())

# # Skjema-SFU: `dsbbase.dlr_enhet_i_delreg_skjema `

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG_SKJEMA
    WHERE DELREG_NR IN ('24{aar2}')
"""
SFU_skjema = pd.read_sql_query(sporring, conn)
print(f"Rader:    {SFU_skjema.shape[0]}\nKolonner: {SFU_skjema.shape[1]}")


SFU = pd.merge(
    SFU_skjema,
    SFU_enhet,
    how='left',
    on='IDENT_NR',
    suffixes=("_skj","_enh"),
    indicator="_kobling"
)

SFU._kobling.value_counts()

SFU['NAVN_NY'] = ""

for navn_kol in ["NAVN1", "NAVN2", "NAVN3", "NAVN4", "NAVN5"]:
    SFU['NAVN_NY'] += SFU[navn_kol].fillna("") + " "

SFU['NAVN_NY'] = SFU['NAVN_NY'].str.strip()

(
    SFU[
        (SFU['SKJEMA_TYPE_skj'].isna()) & (SFU['SKJEMA_TYPE_skj'] != "HELSE39")]
    .groupby(['ORGNR_FORETAK', 'ORGNR', 'NAVN_NY'])
    .agg(skjema=('SKJEMA_TYPE_skj', 'unique'),
         ant_rader=('IDENT_NR', 'count')).reset_index().sort_values('ant_rader', ascending=False)
)

# # Purring Februar:

# Vi skal purre på virksomheter som:
#
# - [x] ikke har `HELSE39` i `SKJEMA_TYPE` i SFU_skjema (ta bort)
# - [x] `HELSE48` **HVIS** foretak er privat. Alle andre skjema skal purres på
# - [x] blanke på `KVITT_TYPE` i SFU_skjema
#
#

# ## Velger alle private skjema:
# > Merk at 39 er kun for private, men har senere leveringsfrist. Filtreres ut her:

# +
skjema_til_purring = ['HELSE38P', 'HELSE44P', 'HELSE45P',
                      'HELSE46P', 'HELSE47', 'HELSE48']

purring_df = SFU[SFU['SKJEMA_TYPE_skj'].isin(skjema_til_purring)]
# -

purring_df.shape

# ### Tar bort de som både skal levere skjema48 og ikke er på oppdrags og bestillerdokument:

både_SKJ48_og_ikke_oppdrag_maske = (purring_df["SKJEMA_TYPE_skj"] == "HELSE48") & (
    purring_df["H_VAR2_A"] != "OPPDRAG"
)

purring_df = purring_df[~både_SKJ48_og_ikke_oppdrag_maske].copy()

# ### Tar bort alle som ikke har noe oppført på `KVITT_TYPE_skj` 

# +
purring_df['NAVN_NY'] = ""

for navn_kol in ["NAVN1", "NAVN2", "NAVN3", "NAVN4", "NAVN5"]:
    purring_df['NAVN_NY'] += purring_df[navn_kol].fillna("") + " "

purring_df['NAVN_NY'] = purring_df['NAVN_NY'].str.strip()
# -

purring_df = purring_df[purring_df['KVITT_TYPE_skj'].isna()]

# +
print("Tre tilfeldige eksempler på virksomheter som skal få purring og på hvilket skjema:")
display(purring_df[['NAVN_NY', 'ORGNR', 'ORGNR_FORETAK', 'SKJEMA_TYPE_skj', 'KVITT_TYPE_skj']].sample(3))
print(f"Antall virksomheter i SFU som har tomme felt på KVITT_TYPE er: {purring_df.shape[0]}")

foretak_til_purring = purring_df.ORGNR_FORETAK.unique()
print(f"Disse fordeler seg på {len(foretak_til_purring)} unike foretak.")
# -

purring_df.groupby('SKJEMA_TYPE_skj').agg(
    ant_foretak=('ORGNR_FORETAK', 'nunique'),
    ant_virk=('ORGNR', 'nunique'),
).reset_index().style.set_caption("Gjenstående foretak og virksomheter per skjema")

purring_df.groupby(['NAVN_NY']).agg(
    ant_foretak=('ORGNR_FORETAK', 'count'),
    ant_virk=('ORGNR', 'count'),
).reset_index()

# # Delregister 20877xx

# Dette er et eget delregister opprettet for kommunikasjon med de private via Altinn. Alle foretak som ikke har blitt innkvittert godkjent, vil få purring. Dvs. alle med missing på KVITT_TYPE får purring.

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar2}')
"""
altinn = pd.read_sql_query(sporring, conn)
print(f"Rader:    {altinn.shape[0]}\nKolonner: {altinn.shape[1]}")


# Filtrerer inn alle foretak som skal få purring i Altinn-delregisteret.



foretak_til_purring_i_altinn_df = altinn[altinn.ORGNR.isin(foretak_til_purring)].ORGNR.to_numpy()
print("Disse skal være tomme set():")
print("Foretak som er markert for purring, men som ikke er i 20877xx:\n",
      set(foretak_til_purring) - set(foretak_til_purring_i_altinn_df))
print("Foretak som er i 20877xx, men som ikke finnes i SFU:\n",
      set(foretak_til_purring_i_altinn_df) - set(foretak_til_purring))



SFU_skjema[['DELREG_NR', 'IDENT_NR', 'ENHETS_TYPE']].sort_values('IDENT_NR')



# Ettersom vi ønsker å sende purring til de som er innkvittert, må vi invertere listene. Dvs, at foretak i delreg `20877XX` som som IKKE er i listen `foretak_til_purring`, skal innkvitteres.

# # Skrive til delregisteret

print("Foretak som nå har levert, men som enda ikke er innkvittert i databasen:")
(
    altinn[
        (~altinn['ORGNR'].isin(foretak_til_purring)) &
        (altinn.KVITT_TYPE.isna())
    ]
    [['NAVN',
      'ORGNR',
      'KVITT_TYPE'
     ]]
)

# +
foretak_til_innkvittering_df = altinn[~altinn['ORGNR'].isin(foretak_til_purring)]  # de som ikke skal ha purring, innkvitteres

foretak_til_innkvittering_df = (
    foretak_til_innkvittering_df
    [
        [
         "DELREG_NR",
         "IDENT_NR",
         "ENHETS_TYPE",
         "KVITT_TYPE",
         "KVITT_FORMAT",
         "DATO_INNKVITTERING"
         ]
    ]
)

# Filtrerer ut de som allerede er inn-/utkvittert på andre måter:
na_mask = foretak_til_innkvittering_df.KVITT_TYPE.isna()
foretak_til_innkvittering_df = foretak_til_innkvittering_df[na_mask].copy()
# -

# Setter nye verdier på kolonnene som skal endres
foretak_til_innkvittering_df['KVITT_TYPE'] = 'K'
foretak_til_innkvittering_df['KVITT_FORMAT'] = 'O'
foretak_til_innkvittering_df['DATO_INNKVITTERING'] = pd.to_datetime(dt.date.today())

foretak_til_innkvittering_df

sql_ins = (
    "INSERT INTO DSBBASE"
    ".DLR_KVITTER_TMP("
    "DELREG_NR,IDENT_NR,ENHETS_TYPE,"
    "KVITT_TYPE,KVITT_FORMAT,DATO_INNKVITTERING"
    ")"
    " VALUES (:1,:2,:3,:4,:5,:6)"
)

print("Dobbeltsjekk sql-spørringen:")
print(sql_ins)

# +
# Oppretter skrivekontakt med Oracle
cur = conn.cursor()

# Stabler om dataframen til SQL-vennlig innlesing
rows = [tuple(x) for x in foretak_til_innkvittering_df.values]
# -

rows

# Hvis til_lagring = True kjøres SQL-inserten
if til_lagring and len(rows) != 0:
    cur.executemany(sql_ins, rows)
    conn.commit()
    print(f"Det er gjort {len(rows)} radendringer. Kontroller i SFU.")

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar2}')
"""
altinn = pd.read_sql_query(sporring, conn)
print(f"Rader:    {altinn.shape[0]}\nKolonner: {altinn.shape[1]}")


print("Siste innkvitterte foretak:")
(
    altinn
    .sort_values(
        'DATO_INNKVITTERING'
        , ascending=False
    )
    [[
        'NAVN',
        'ORGNR',
        'DATO_INNKVITTERING'
      ]]
    .head(10)
)

print(f"Foretak ({altinn[altinn['KVITT_TYPE'].isna()].shape[0]}) som ikke har godkjent innkvittering i delreg 20877xx per {dt.date.today()}:")
(altinn
     [
         altinn
         ['KVITT_TYPE']
         .isna()
      ]
     [[
         'NAVN',
         'ORGNR',
         'DATO_INNKVITTERING'
     ]]
 )

# mask1 = SFU['ORGNR_FORETAK'].isin(foretak_til_purring)
mask2 = SFU['KVITT_TYPE_skj'].isna()
mask3 = ~SFU['SKJEMA_TYPE_skj'].isin(["HELSE39", "HELSE0X", "HELSE0Y", "HELSE40", "RA-0595"])
print(f"Virksomheter som ikke har levert ({SFU[mask2 & mask3].shape[0]}):")
ikke_levert = SFU[mask2 & mask3][['NAVN',
                                          'ORGNR',
                                          'ORGNR_FORETAK',
                                          'SKJEMA_TYPE_skj',
                                          'KVITT_TYPE_skj'
                                          ]].sort_values('ORGNR_FORETAK')
ikke_levert


