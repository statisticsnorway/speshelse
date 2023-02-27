# # Lese fra og skrive til delregister i Oracle

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

# # Vanlig SFU: `dsbbase.dlr_enhet_i_delreg `
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


SFU = pd.merge(
    SFU_skjema,
    SFU_enhet,
    how='left',
    on='IDENT_NR',
    suffixes=("_skj","_enh")
)

SFU[['NAVN', 'ORGNR', 'SKJEMA_TYPE_skj', 'KVITT_TYPE_skj', 'KVITT_TYPE_enh']].sample(3)

# ### Purring Februar:

# Vi skal ha purring på de som er:
#
# - [x] ikke har `HELSE39` i `SKJEMA_TYPE` i SFU_skjema (ta bort)
# - [x] `HELSE48` **HVIS** foretak er privat. Alle andre skjema skal purres på
# - [ ] blanke på `KVITT_TYPE` i SFU_skjema
#
#

# ### Velger alle private skjema:
# > Merk at 39 er for privat, men har senere leveringsfrist. Filtreres ut allerede her

# +
skjema_til_purring = ['HELSE38P', 'HELSE44P', 'HELSE45P',
                      'HELSE46P', 'HELSE47', 'HELSE48']

purring_df = SFU[SFU['SKJEMA_TYPE_skj'].isin(skjema_til_purring)]
# -

# ### Tar bort de som skal levere skjema48 og ikke er private:

maske1 = (purring_df['SKJEMA_TYPE_skj'] == 'HELSE48') & (purring_df['H_VAR2_A'] != 'OPPDRAG')
purring_df = purring_df[~maske1]

# ### Tar bort alle som ikke har noe oppført på `KVITT_TYPE_skj` 

purring_df = purring_df[purring_df['KVITT_TYPE_skj'].isna()]

purring_df[['NAVN', 'ORGNR', 'SKJEMA_TYPE_skj', 'KVITT_TYPE_skj']].sample(3)

print(f"Antall virksomheter i SFU som har tomme felt på KVITT_TYPE er: {purring_df.shape[0]}")

foretak_til_purring = purring_df.ORGNR_FORETAK.unique()

print(f"Disse fordeler seg på {len(foretak_til_purring)} unike foretak.")

# # Delregister 2087722

# Dette er et eget delregister opprettet for kommunikasjon med de private via Altinn. Alle foretak som ikke har blitt innkvittert godkjent, vil få purring.

sporring = f"""
    SELECT *
    FROM DSBBASE.DLR_ENHET_I_DELREG
    WHERE DELREG_NR IN ('20877{aar2}')
""" 
altinn = pd.read_sql_query(sporring, conn)
print(f"Rader:    {altinn.shape[0]}\nKolonner: {altinn.shape[1]}")


# Filtrerer inn alle foretak som skal få purring i Altinn-delregisteret.

foretak_til_purring_i_altinn_df = altinn[altinn.ORGNR.isin(foretak_til_purring)].ORGNR.to_numpy()

print(set(foretak_til_purring) - set(foretak_til_purring_i_altinn_df))
print(set(foretak_til_purring_i_altinn_df) - set(foretak_til_purring))
print("Hvis begge settene er tomme, er alle foretakene vi har tenkt å sende",
      "purring til,\n i Altinn-delregisteret og motsatt.")

# Ettersom vi ønsker å sende purring til de som er innkvittert, må vi invertere listene. Dvs, at foretak i delreg `2087722` som som IKKE er i listen `foretak_til_purring`, skal innkvitteres.

# # Skrive til delregisteret

print("Foretak som nå har levert, men som enda ikke er innkvittert i databasen:")
altinn[(~altinn['ORGNR'].isin(foretak_til_purring)) & (altinn.KVITT_TYPE.isna())][['NAVN', 'ORGNR', 'KVITT_TYPE']].head(5)

foretak_til_innkvittering_df = altinn[~altinn['ORGNR'].isin(foretak_til_purring)]

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

# Hvis til_lagring = True kjøres SQL-inserten
if til_lagring:
    cur.executemany(sql_ins, rows)
    conn.commit()

# -

print(f"Foretak som ikke har godkjent innkvittering per {dt.date.today()}:")
altinn[altinn['KVITT_TYPE'].isna()][['NAVN', 'ORGNR']]

mask1 = SFU['ORGNR_FORETAK'].isin(foretak_til_purring)
mask2 = SFU['KVITT_TYPE_skj'].isna()
mask3 = SFU['SKJEMA_TYPE_skj'] != "HELSE39"
print("Virksomheter som ikke har levert:")
SFU[mask1 & mask2 & mask3][['NAVN',
                            'ORGNR',
                            'ORGNR_FORETAK',
                            'SKJEMA_TYPE_skj',
                            'KVITT_TYPE_skj'
                            ]].sort_values('ORGNR_FORETAK')

# # Skjemaoversikt
# Hvordan ligger skjemainnleveringen an?

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

skjemaoversikt = pd.DataFrame([tot, ant_ikke_levert]).T
skjemaoversikt.columns = ['tot', 'ikke_levert']
skjemaoversikt['levert'] = skjemaoversikt['tot'] - skjemaoversikt['ikke_levert']
skjemaoversikt['prosentandel_levert'] = round(skjemaoversikt['levert'] / skjemaoversikt['tot'] * 100)
skjemaoversikt = skjemaoversikt.sort_values('prosentandel_levert', ascending=False)
skjemaoversikt

skjemaoversikt.prosentandel_levert.plot(kind='bar', grid=True, ylim=(0,100), title="Prosentvis andel virksomheter som har levert:")

print(altinn[altinn['KVITT_TYPE'].isna()][['NAVN','ORGNR']].shape)
display(altinn[altinn['KVITT_TYPE'].isna()][['NAVN','ORGNR']])

altinn.shape
