import pandas as pd
import numpy as np
from sqlalchemy import text

def lag_sql_str(arr):
    s = "("
    for nr in arr:
        s += "'" + str(nr) + "',"
    s = s[:-1] + ")"
    return s


def hent_enheter_fra_klass(aargang):
    """
    Henter rapporteringsenheter fra KLASS basert på gitt årgang.

    Denne funksjonen henter data fra KLASS for offentlige helseforetak,
    private helseinstitusjoner med oppdrags- og bestillerdokument, og 
    regionale og felleseide støtteforetak i spesialisthelsetjenesten.

    Args:
    - aargang (str): Året det hentes data for, i formatet 'YYYY'.

    Returns:
    - tuple: Returnerer syv DataFrames som representerer ulike rapporteringsenheter:
        - HF: Data for offentlige helseforetak.
        - RHF: Data for regionale helseforetak.
        - phob: Data for private helseinstitusjoner med oppdrags- og bestillerdokument.
        - rfss: Data for regionale og felleseide støtteforetak.
        - rfss2: Data for støtteforetak med parentCode '99'.
        - rfss3: Data for støtteforetak uten parentCode '99'.
        - rapporteringsenheter: Samlet data for alle rapporteringsenheter.

    Eksempel:
    >>> HF, RHF, phob, rfss, rfss2, rfss3, rapporteringsenheter = hent_enheter_fra_klass('2023')
    """

    from klass import get_classification

    ### Offentlige helseforetak

    klass_offentlige_helseforetak = get_classification(603).get_codes(from_date=f'{aargang}-01-01',
                                                                      to_date=f'{aargang}-01-02')

    HF = klass_offentlige_helseforetak.pivot_level()
    HF = (
        HF.rename(columns={
            'code_1': 'HELSEREGION',
            'code_3': 'ORGNR_FORETAK',
            'name_3': 'NAVN_KLASS'
        })
        [['ORGNR_FORETAK', 'NAVN_KLASS', 'HELSEREGION']]
        .reset_index(drop=True)
    )

    RHF = klass_offentlige_helseforetak.pivot_level()
    RHF = (
        RHF[['code_1', 'name_2', 'code_2']]
        .drop_duplicates()
        .rename(columns={'code_1': 'HELSEREGION',
                         'name_2': 'NAVN_KLASS',
                         'code_2': 'ORGNR_FORETAK'})
        .reset_index(drop=True)
    )

    ### Private helseinstitusjoner med oppdrags- og bestillerdokument

    klass_priv_frtk_ob = get_classification(604).get_codes(from_date=f'{aargang}-01-01',
                                                           to_date=f'{aargang}-01-02')

    phob = klass_priv_frtk_ob.pivot_level()

    phob = (
        phob.rename(columns={
            'code_1': 'HELSEREGION',
            'code_2': 'ORGNR_FORETAK',
            'name_2': 'NAVN_KLASS'
        })
        [['ORGNR_FORETAK', 'NAVN_KLASS', 'HELSEREGION']]
        .reset_index(drop=True)
    )

    ### Regionale og felleseide støtteforetak i spesialisthelsetjenesten

    klass_rfss = get_classification(605).get_codes(from_date=f'{aargang}-01-01',
                                                   to_date=f'{aargang}-01-02')

    rfss = klass_rfss.pivot_level()

    rfss = (
        rfss.rename(columns={
            'code_1': 'HELSEREGION',
            'code_3': 'ORGNR_FORETAK',
            'name_3': 'NAVN_KLASS'
        })
        [['ORGNR_FORETAK', 'NAVN_KLASS', 'HELSEREGION']]
        .reset_index(drop=True)
    )

    rfss2 = (
        get_classification(605)
        .get_codes(from_date=f'{aargang}-01-01',
                   to_date=f'{aargang}-01-02')
        .data.query("level == '2' & parentCode == '99'")
        [['code', 'parentCode', 'name']]
        .rename(columns={
            'code': 'ORGNR_FORETAK',
            'parentCode': 'HELSEREGION',
            'name': 'NAVN_KLASS'
            }
        )
        .reset_index(drop=True)
    )

    rfss3 = (
        get_classification(605)
        .get_codes(from_date=f"{aargang}-01-01", to_date=f"{aargang}-01-02")
        .data
    )

    RHF_orgnr_list = RHF.ORGNR_FORETAK.to_list()
    m1 = rfss3["level"] == "2"
    m2 = rfss3["parentCode"] != "99"
    m3 = ~rfss3["code"].isin(RHF_orgnr_list)

    rfss3 = rfss3[m1 & m2 & m3][['code', 'parentCode', 'name']].rename(
        columns={"code": "ORGNR_FORETAK", "parentCode": "HELSEREGION", "name": "NAVN_KLASS"}
    ).reset_index(drop=True)

    rfss_region = klass_rfss.data.copy()

    rfss_region = (
        rfss_region[rfss_region['level'] == '1'][['code', 'name']]
        .rename(columns={'code': 'HELSEREGION', 'name': 'RHF'})
    )

    rapporteringsenheter = pd.concat(
        [HF, RHF, phob, rfss, rfss2, rfss3]
    )

    rapporteringsenheter = pd.merge(rapporteringsenheter,
                                    rfss_region,
                                    how='left',
                                    on='HELSEREGION'
                                    )

    rapporteringsenheter = rapporteringsenheter.rename(columns={'RHF': 'HELSEREGION_NAVN'}).reset_index(drop=True)

    rapporteringsenheter = rapporteringsenheter[~rapporteringsenheter.duplicated()]

    return HF, RHF, phob, rfss, rfss2, rfss3, rapporteringsenheter



def les_sql(sql_spørring, tilkobling):
    """
    Utfører en SQL-spørring og returnerer en DataFrame hvor kolonnenavnene er i store bokstaver.
    
    Parametere:
    - sql_spørring (str): SQL-spørringen som skal utføres.
    - tilkobling (SQLAlchemy connection): Databaseforbindelsen som skal brukes for spørringen.
    
    Returnerer:
    - DataFrame: Resultatet av SQL-spørringen med kolonnenavnene i store bokstaver.
    """

    # Utfør SQL-spørringen
    df = pd.read_sql_query(text(sql_spørring), tilkobling)

    # Konverter kolonnenavnene til store bokstaver
    df.columns = [col.upper() for col in df.columns]

    return df


def hent_data_delreg24x_og_19377x(x, skjema, conn):
    sporring = f"""
        SELECT *
        FROM DYNAREV.VW_SKJEMA_DATA
        WHERE DELREG_NR IN ('24{x}', '19377{x}') AND SKJEMA IN ('{skjema}') AND AKTIV = '1'
    """
    sporring_df = les_sql(sporring, conn)
    sporring_df = sporring_df[['SKJEMA','ENHETS_ID', 'FELT_ID', 'FELT_VERDI']]
    sporring_df = sporring_df.pivot(index=['ENHETS_ID','SKJEMA'], columns='FELT_ID', values='FELT_VERDI')
    return sporring_df


def hent_foretaksnavn_til_virksomhetene_fra_SFU(unike_foretak, SFUklass):
    unike_foretak = pd.DataFrame(unike_foretak)
    unike_foretak.columns = ["ORGNR_FORETAK"]

    finn_foretaksnavn = pd.merge(unike_foretak, SFUklass[["ORGNR", "NAVN"]],
                                 how="left",
                                 left_on="ORGNR_FORETAK",
                                 right_on="ORGNR")
    finn_foretaksnavn = finn_foretaksnavn[["ORGNR_FORETAK", "NAVN"]]\
                                        .rename(columns={"NAVN": "FORETAK_NAVN",
                                        "ORGNR_FORETAK": "FORETAK_ORGNR"})
    return finn_foretaksnavn


def lag_navn_orgnr_kolonnenavn(ant_kolonner):
    """
    Denne funksjonen tar inn en dataframe med undervirksomheter som hører til et
    foretaksnummer og antall kolonner som skal være med i droplisten.
    Undervirksomhetene legges etterhverandre med riktig korrespondanse
    mellom navn- og orgnr-kolonner.
    """
    nvn, org = [], []
    for i in range(1,ant_kolonner+1):
        nvn.append(f'NAVN_VIRK{i}')
        org.append(f'ORGNR_VIRK{i}')
    return nvn, org


def lag_navn_orgnr_kolonner(foretak_virk_df, ant_kolonner, med_foretak=True):
    """
    Denne funksjonen tar en pandas DataFrame (foretak_virk_df) som inneholder
    informasjon om organisasjoner og deres foretaksnummer (ORGNR_FORETAK).
    Den oppretter en ny DataFrame der hver unike organisasjon har en
    egen kolonne for organisasjonsnummer (ORGNR) og navn (NAVN).

    Parametere:
    - foretak_virk_df (pandas DataFrame): Inndataframe som inneholder organisasjonsdata.
    - ant_kolonner (int): Antall kolonner i den resulterende DataFrame for hver
      organisasjon.
    - med_foretak (bool, valgfritt): En boolsk verdi som angir om kolonnene for
      organisasjonsnummer og navn skal inkluderes.
      Hvis False, ekskluderes organisasjonsnummer i kolonner hvis de samsvarer med ORGNR_FORETAK.

    Returnerer:
    - En ny DataFrame der hver unike organisasjon har en kolonne for ORGNR og NAVN. Kolonnene kan være tomme (NaN) hvis det ikke er nok data.
    - DataFramen er transponert for å ha organisasjoner som indekser i stedet for kolonner.
    """
    unique_orgnr = foretak_virk_df['ORGNR_FORETAK'].unique()

    columns = [f"ORGNR_VIRK{i+1}" for i in range(ant_kolonner)] + \
              [f"NAVN_VIRK{i+1}" for i in range(ant_kolonner)] + ["ORGNR_FORETAK"]
    df_result = pd.DataFrame(columns=columns)
    
    for nr in unique_orgnr:
        subset = foretak_virk_df[foretak_virk_df['ORGNR_FORETAK'] == nr]
        
        if not med_foretak:
            subset = subset[subset['ORGNR'] != nr]

        orgnr_values = subset['ORGNR'].tolist() + [np.nan] * (ant_kolonner - len(subset))
        navn_values = subset['NAVN'].tolist() + [np.nan] * (ant_kolonner - len(subset))
        
        row_values = orgnr_values + navn_values + [nr]
        temp_df = pd.DataFrame([row_values], columns=columns)
        df_result = pd.concat([df_result, temp_df], ignore_index=True)
    
    return df_result


def instlist_med_riktig_antall_n(finst_orgnr_df, SFUklass):
    """
    Denne funksjonen utfører en serie operasjoner på DataFrames og returnerer to DataFrames.

    Parameters:
        SFUklass (pd.DataFrame): En DataFrame med data.
        finst_orgnr_df (pd.DataFrame): En annen DataFrame med data.

    Returns:
        rapporterer_ikke_til_annen_v (pd.DataFrame): En DataFrame som inneholder organisasjonsnummer og tilhørende navn for enheter som ikke rapporterer til en annen enhet.
        df (pd.DataFrame): En DataFrame som inneholder organisasjonsnummer og en liste over enheter som rapporterer til en annen enhet.

    Funksjonen utfører følgende operasjoner:
    1. Utfør en "merge" operasjon mellom SFUklass og finst_orgnr_df basert på kolonnene "H_VAR1_A" og "FINST_ORGNR."
    2. Legg til en ny kolonne 'rapporterer_til_annen_virksomhet' som angir om organisasjonsnummeret er forskjellig fra 'H_VAR1_A'.
    3. Velg bestemte kolonner fra den resulterende DataFrame.
    4. Lag en ny kolonne 'orgnr_navn' som kombinerer organisasjonsnummeret og navnet.
    5. Opprett to nye DataFrames 'rapporterer_ikke_til_annen_v' og 'rapporterer_til_annen_v' ved hjelp av spørringer.
    6. Gjennomfør en midlertidig operasjon for å legge institusjoner horisontalt ved siden av rapporteringsnummeret og opprett en DataFrame 'df' for dette.
    7. Returner 'rapporterer_ikke_til_annen_v' og 'df' DataFrames som resultat.

    Merk: Hvis 'df' ikke er None, blir kolonnene omdøpt til 'FINST_ORGNR' og 'INSTLISTE_HALE' før retur.
    """
    df_til_instlist = pd.merge(SFUklass, finst_orgnr_df, left_on="H_VAR1_A", right_on="FINST_ORGNR")
    df_til_instlist['rapporterer_til_annen_virksomhet'] = (df_til_instlist.ORGNR != df_til_instlist.H_VAR1_A)
    df_til_instlist = df_til_instlist[['NAVN', 'ORGNR', "FINST_ORGNR", 'rapporterer_til_annen_virksomhet']]
    df_til_instlist['orgnr_navn'] = df_til_instlist['ORGNR'] + " - " + df_til_instlist['NAVN']

    rapporterer_ikke_til_annen_v = df_til_instlist.query('~rapporterer_til_annen_virksomhet')[['ORGNR', 'orgnr_navn']]
    rapporterer_til_annen_v = df_til_instlist.query('rapporterer_til_annen_virksomhet')[["FINST_ORGNR", 'orgnr_navn']]
    
    # mellomsteg for å legge institusjonene horisontalt ved siden av rapporteringsnummeret
    # itererer over alle unike rapporteringsnummer
    df = None
    for finst_orgnr in rapporterer_ikke_til_annen_v.ORGNR.unique():
        instliste = "\\n".join(rapporterer_til_annen_v.query(f'FINST_ORGNR=="{finst_orgnr}"').orgnr_navn)
        if len(instliste) > 0:
            if df is None:
                df = pd.DataFrame([finst_orgnr, instliste])
            else:
                nytt_tillegg = pd.DataFrame([finst_orgnr, instliste])
                df = pd.concat([df, nytt_tillegg], axis=1)
    
    if df is not None:
        df = df.transpose()
        df.columns = ['FINST_ORGNR', 'INSTLISTE_HALE']   
    
    rapporterer_ikke_til_annen_v = rapporterer_ikke_til_annen_v.rename(columns={'ORGNR': 'FINST_ORGNR'})
    return rapporterer_ikke_til_annen_v, df


def tabell_som_inneholder_skjema(df, kol, skjemanavn):
    """
    Funksjon som returnerer en tabell med alle rader som inneholder et gitt skjema
    """
    return df[df[kol].apply(lambda x: skjemanavn in x)]


def legg_paa_hale_med_n(df):
    """
    Lager kolonnen INSTLIST med riktig antall \n for at det skal passe med skjema.
    Passer på at rapporteringsenheten kommer først i listen.
    (Enhet som har H_VAR1_A lik orgnummer)
    """
    df['INSTLISTE_HALE'] = df['INSTLISTE_HALE'].fillna("\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n\\n")
    df['antall_n_mangler'] = df['INSTLISTE_HALE'].apply(lambda x: 11-x.count("\\n"))
    df['INSTLIST'] = df.apply(lambda x: x.orgnr_navn + \
                                                "\\n" + \
                                                x.INSTLISTE_HALE + \
                                                x.antall_n_mangler * ("\\n"), axis=1)
    return df


