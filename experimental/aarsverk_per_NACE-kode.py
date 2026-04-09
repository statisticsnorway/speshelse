# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.15.2
#   kernelspec:
#     display_name: Python3
#     language: python
#     name: python3
# ---

# # Introduksjon

# ## Bestilling
#
# Epost fra Terje Landsem, K 425 Seksjon for energi, miljø- og transport 
#
# ```
# Hei igjen,
#
# Skulle hatt noe data på årsverk for helsetjenester, (årsverk for nace: 86.101, 86.102 og 86.906) til statistikken over farlig avfall vi jobber med. Det er tilsvarende tall for som for 2020, for 2021-årgangen vi behøver for farlig avfall. Helst så fort som mulig 😊
#
# Mvh Terje Landsem
# ```
#

# + [markdown] toc-hr-collapsed=true
# # Setup
# -

# ## Velger årgang

# +
aargang = 2024

# Næringskoder som skal med
naring = ["86.101", "86.102", "86.906"]

# Kolonner fra sysskostra
cols = ["VIRK_ID_SSB", "VIRK_NAVN", "FRTK_ID_SSB", "FRTK_NAVN", "VIRK_NACE1_SN07", "ARB_AARSVERK", "PERS_LANGEFRA_ANDEL", "VIRK_HO_MERKE"]
# -

cols = [col.lower() for col in cols]

# ## Importerer pakker og setter innstillinger

# +
import numpy as np
import pandas as pd

# Fjerner begrensning på antall rader og kolonner som vises av gangen
pd.set_option("display.max_columns", None)
pd.set_option('display.max_rows', 300)
pd.set_option('display.max_colwidth', None)

# Unngå standardform i output
pd.set_option('display.float_format', lambda x: '%.1f' % x)
# -

# ## Laster inn personell rådata for personelldata (sysskostra)

sti = f"/ssb/stamme01/fylkhels/speshelse/personell/{aargang}/personell/inndata/g{aargang}_sysskostra.parquet"
data = pd.read_parquet(sti)

# # Virksomheter markert med SPES(helse)

data.sample(5)

# +
df = data[cols].copy()

# Formel for å regne ut årsverk. (Trekker fra langtidsfravær)
df['arsverk_ekskl'] = df['arb_aarsverk']-(df['arb_aarsverk']*df['pers_langefra_andel'])
# -

df = df.loc[df['virk_nace1_sn07'].isin(naring)]
df = df.loc[df['virk_ho_merke'].isin(["SPES"])]

df.sample(5)

df = df.drop(columns=['arb_aarsverk', 'pers_langefra_andel'])

# +
virk_i_delreg = df.groupby(['virk_id_ssb',
                            'virk_navn',
                            'frtk_id_ssb',
                            'frtk_navn',
                            'virk_nace1_sn07']
                            ).sum(numeric_only=True)
virk_i_delreg = virk_i_delreg.reset_index()

virk_i_delreg = virk_i_delreg.rename(columns = {'arsverk_ekskl': 'sum_arsverk_ekskl'})
virk_i_delreg['sum_arsverk_ekskl'] = virk_i_delreg['sum_arsverk_ekskl'].astype(str).apply(lambda x: x.replace(".", ","))
# -

df.dtypes

virk_i_delreg.head(5)

virk_i_delreg.shape

# # Virksomheter ikke markert med SPES(helse)

# +
df = data[cols].copy()

# Formel for å regne ut årsverk. (Trekker fra langtidsfravær)
df['arsverk_ekskl'] = df['arb_aarsverk']-(df['arb_aarsverk']*df['pers_langefra_andel'])
# -

df = df.loc[df['virk_nace1_sn07'].isin(naring)]
df = df.loc[~df['virk_ho_merke'].isin(["SPES"])]

df

# +
df = df.drop(columns=['arb_aarsverk', 'pers_langefra_andel'])

virk_ikke_i_delreg = df.groupby(['virk_id_ssb',
                                 'virk_navn',
                                 'frtk_id_ssb',
                                 'frtk_navn',
                                 'virk_nace1_sn07']
                                 ).sum(numeric_only=True)
virk_ikke_i_delreg = virk_ikke_i_delreg.reset_index()

virk_ikke_i_delreg = virk_ikke_i_delreg.rename(columns = {'arsverk_ekskl': 'sum_arsverk_ekskl'})
virk_ikke_i_delreg['sum_arsverk_ekskl'] = virk_ikke_i_delreg['sum_arsverk_ekskl'].astype(str).apply(lambda x: x.replace(".", ","))
# -

virk_ikke_i_delreg.head(5)

virk_ikke_i_delreg.shape


# # Eksportering til .csv-filer og lagring
# Filene lagres i home/jovian/data_temp

def lagre_fil(skj):
    dato_idag = pd.Timestamp("today").strftime("%d%m%y")
    stamme="../../data_temp/"
    filnavn = "aarsverk_etter_nace_" + skj + "_" + str(aargang) + "_" + dato_idag + ".csv"
    eval(skj).to_csv(stamme + filnavn, sep=";", encoding='utf-8', index=False)
    print(filnavn, " lagret")


lagre_fil("virk_i_delreg")
lagre_fil("virk_ikke_i_delreg")

virk_ikke_i_delreg
