# # Brukerlister
# Dette er en oversikt over alle foretak som skal ha skjemaer og hvilke skjemaer de skal få. Også kalt _populasjonsliste_ av KOSTRA-IT.
#
# - Alle hjelpeforetak som skal rapportere skal ha skjema: `"0X 0Y 40"`
#     - Dette ser ut til å være alle som ikke er apotek. Foretaksnummerne til disse er listet opp under.
# - Alle RHF skal ha skjema: `"0X 0Y 40 41"`
# - Alle HF skal ha skjema: `"0X 0Y 40 380 440 450 460 48"`
# - Alle foretak med oppdrag og bestillerdokument skal ha skjema: `"39 381 441 451 461 47 48"`
# - Alle private foretak skal ha skjema: `"39 381 441 451 461 47"`

aar4 = 2022

import pandas as pd
from klass import klass_df
from klass import klass_df_wide
from klass import klass_get

# ## Hjelpeforetak

hjelpeforetak_som_ikke_er_rapporteringsenhet = ['918098275', '983974716', '983974805', '983974937', '992281618']

URL = f'https://data.ssb.no/api/klass/v1/classifications/605/codes.json?from={aar4}-01-01&includeFuture=True'
regfel_df = klass_df(URL, level='codes')

# +
# tar ut RHFene fra listen
df_hj = regfel_df.query("level != '1' and parentCode not in ['03','04','05','12']") 

# tar bort unntakene
df_hj = df_hj[~df_hj['code'].isin(hjelpeforetak_som_ikke_er_rapporteringsenhet)]
# -

df_hj['s'] = "0X 0Y 40"
df_hj = df_hj[['name', 'code', 's']]
df_hj.columns = ['FORETAK_NAVN', 'ORGNR_FORETAK', 'SKJEMA_TYPE']

df_hj

# ## RHF

df_rhf = regfel_df[regfel_df['parentCode'].isin(['03','04','05','12'])].copy()

df_rhf['s'] = "0X 0Y 40 41"
df_rhf = df_rhf[['name', 'code', 's']]
df_rhf.columns = ['FORETAK_NAVN', 'ORGNR_FORETAK', 'SKJEMA_TYPE']

df_rhf


# ## HF

def klass_df_wide(URL):
    df_raw = (
        klass_get(URL,
                  level='codes',
                  return_df=True)
                  [['code', 'parentCode', 'level', 'name']]
        )
    lowest_level = int(df_raw.level.unique().max())
    df_list = []
    for i in range(1, lowest_level+1):
        temp = df_raw[df_raw['level'] == f'{i}'].copy()
        temp.columns = [f'code_{i}', f'parentCode_{i}', 'level', f'name_{i}']
        df_list.append(temp)

    df_wide = df_list[0].copy()

    for i in range(0, len(df_list)-1):
        this_lvl = i+1
        child_lvl = i+2
        
    # for i in range(1, len(df_list)):
    #     parent_lvl = lowest_level - i
    #     this_lvl = parent_lvl + 1
    #     df_list[i] = df_list[i].drop(columns=['level'])
    #     df_wide = pd.merge(
    #          df_wide,
    #          df_list[i],
    #          how='left',
    #          left_on=f'parentCode_{this_lvl}',
    #          right_on=f'code_{parent_lvl}'
    #     )
    return df_wide


URL = f'https://data.ssb.no/api/klass/v1/classifications/605/codes.json?from={aar4}-01-01&includeFuture=True'
HF = klass_df_wide(URL)

HF

df_hf = HF.copy()

df_hf['s'] = "0X 0Y 40 380 440 450 460 48"
df_hf = df_hf[['NAVN_KLASS', 'ORGNR_FORETAK', 's']]
df_hf.columns = ['FORETAK_NAVN', 'ORGNR_FORETAK', 'SKJEMA_TYPE']

# ## Private

df_p = SFUklass[SFUklass['H_VAR2_A'] == 'PRIVAT'].copy()

df_p['s'] = "39 381 441 451 461 47"

navn_orgnr_df = SFUklass[['NAVN', 'ORGNR']]

sql_str = lag_sql_str(df_p.ORGNR_FORETAK.unique())

sporring_for = f"""
    SELECT *
    FROM DSBBASE.SSB_FORETAK
    WHERE STATUSKODE = 'B' AND ORGNR IN {sql_str}
"""
navn_vof = pd.read_sql_query(sporring_bed, conn)

rapporteringsenheter_vof[rapporteringsenheter_vof['ORGNR_FORETAK'] == "944384448"]

vof[vof['ORGNR_FORETAK']=="944384448"]

rapporteringsenheter_vof[rapporteringsenheter_vof['ORGNR_BEDRIFT'] == rapporteringsenheter_vof['ORGNR_FORETAK']]

df_p = pd.merge(
    df_p,
    navn_vof,
    how='left',
    left_on='ORGNR_FORETAK',
    right_on='ORGNR'
)

df_p = df_p[['NAVN_x', 'NAVN_y', 'ORGNR_FORETAK', 's']]

# +
# df_p.columns = ['FORETAK_NAVN', 'ORGNR_FORETAK', 'SKJEMA_TYPE']
# -

print("Med duplikater: ", df_p.shape)

df_p = df_p.drop_duplicates(subset=['ORGNR_FORETAK'], keep='first')
print("Uten duplikater: ", df_p.shape)

# ## Oppdrags- og bestillerdokument

# +
df_op = SFUklass[SFUklass['H_VAR2_A'] == 'OPPDRAG'].copy()

df_op['s'] = "39 381 441 451 461 47 48"
df_op['NYTT_NAVN'] = df_op['NAVN1'] + " " + df_op['NAVN2']
df_op = df_op[['NYTT_NAVN', 'ORGNR_FORETAK', 's']]
df_op.columns = ['FORETAK_NAVN', 'ORGNR_FORETAK', 'SKJEMA_TYPE']
print("Med duplikater: ", df_op.shape)
df_op = df_op.drop_duplicates(subset=['ORGNR_FORETAK'], keep='first')
print("Uten duplikater: ", df_op.shape)
# -

# ## Slå sammen til en dataframe som eksporteres til `.csv`

brukerliste_dfs = [df_hj, df_rhf, df_hf, df_p, df_op]
brukerliste_df = pd.concat(brukerliste_dfs)

