import pandas as pd

sti = "/ssb/stamme01/fylkhels/speshelse/felles/droplister/2022/061222/Dropliste_skj39_2022_061222.csv"

df = pd.read_csv(sti, sep=";", encoding='latin1')

df
