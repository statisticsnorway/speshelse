import pandas as pd
import os

def lagre_excel(dfs, sti, inkluder_indeks=False):
    """
    Lagrer en eller flere DataFrames i en Excel-fil på den angitte banen (sti).

    Args:
        dfs (dict): En dictionary der nøklene er arknavn og verdiene er DataFrames som skal lagres i Excel-filen.
        sti (str): Stien til Excel-filen der DataFrames skal lagres.
        inkluder_indeks (bool): Hvis True, inkluderer DataFrame-indeksen i Excel-filen.

    Eksempel:
        For å lagre to DataFrames 'df1' og 'df2' i en Excel-fil 'eksempel.xlsx' på stien '/sti/til/fil/eksempel.xlsx':

        >>> import pandas as pd
        >>> data1 = {'A': [1, 2, 3], 'B': [4, 5, 6]}
        >>> data2 = {'X': [7, 8, 9], 'Y': [10, 11, 12]}
        >>> df1 = pd.DataFrame(data1)
        >>> df2 = pd.DataFrame(data2)
        >>> dataframes = {'Ark1': df1, 'Ark2': df2}
        >>> sti_til_fil = '/sti/til/fil/eksempel.xlsx'
        >>> lagre_excel(dataframes, sti_til_fil)

        Funksjonen vil opprette en Excel-fil på '/sti/til/fil/eksempel.xlsx' og lagre 'df1' i 'Ark1'-arket og 'df2' i 'Ark2'-arket i filen.
    Merk:
        Arknavn som er lengre enn 30 tegn vil bli forkortet automatisk.
    """
    # Opprett mappen hvis den ikke eksisterer
    mappe_sti = os.path.dirname(sti)
    if not os.path.exists(mappe_sti):
        try:
            os.makedirs(mappe_sti)
            print(f"Opprettet mappen: {mappe_sti}")
        except Exception as e:
            print(f"Kunne ikke opprette mappen: {e}")
            raise
    
    try:
        with pd.ExcelWriter(sti, engine='openpyxl') as writer:
            for arknavn, df in dfs.items():
                # Forkort arknavn hvis nødvendig
                arknavn = (arknavn[:27] + '...') if len(arknavn) > 30 else arknavn

                df.to_excel(writer, sheet_name=arknavn, index=inkluder_indeks)
                print(f"Arkfanen '{arknavn}' er lagret i filen.")

    except Exception as e:
        print(f"Feil oppstod ved lagring av Excel-fil: {e}")
        raise

    print(f"Excel-filen er lagret på sti: {sti}")
