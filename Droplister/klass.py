import requests
import json
import pandas as pd


def klass_get(URL: str, level: str, return_df=False):
    """
    Parameter1: URL, the uri to a KLASS-API endpoint. Like 
    https://data.ssb.no/api/klass/v1/classifications/533/codes.json?from=2020-01-01&includeFuture=True
    Parameter2: Level, defined by the API, like "codes"
    Parameter3: return_df, returns json if set to False, a pandas dataframe if True
    Returns: a pandas dataframe with the classification
    """
    if URL[:8] != "https://":
        raise requests.HTTPError("Please use https, not http.")
    r = requests.get(URL)
    # HTTP-errorcode handling
    if r.status_code != 200:
        raise requests.HTTPError(f"Connection error: {r.status_code}. Try using https on Dapla?")
    # Continue munging result
    r = json.loads(r.text)[level]
    if return_df:
        return pd.json_normalize(r)
    return r


def klass_df(URL: str, level: str):
    """
    By using this function to imply that you want a dataframe back.
    Parameter1: URL, the uri to a KLASS-API endpoint. Like 
    https://data.ssb.no/api/klass/v1/classifications/533/codes.json?from=2020-01-01&includeFuture=True
    Parameter2: Level, defined by the API, like "codes"
    Returns: a pandas dataframe with the classification
    """
    return klass_get(URL, level, return_df=True)


def correspondance_dict(corr_id: str) -> dict:
    """Get a correspondance from its ID and
    return a dict of the correspondanceMaps["sourceCode"] as keys
    to the correspondanceMaps["targetCode"] as values.
    Apply this to a column in pandas with the .map method for example."""
    if isinstance(corr_id, float):
        corr_id = int(corr_id)
    if isinstance(corr_id, int):
        corr_id = str(corr_id)
    url = 'https://data.ssb.no/api/klass/v1/correspondencetables/' + corr_id
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers=headers).text
    corr = json.loads(response)["correspondenceMaps"]
    return {s: t for s, t in zip([x["sourceCode"] for x in corr],
                                 [x["targetCode"] for x in corr])}


def search_classifications(searchterm: str, page: int = 0, size: int = 20) -> list:
    url = ("https://data.ssb.no/api/klass/v1/classifications/search?query=" + 
           searchterm +
          "&page=" + page +
          "&size=" + size)
    headers = {'Accept': 'application/json'}
    response = requests.get(url, headers=headers).text
    search_result = json.loads(response)
    for result in search_result["_embedded"]["searchResults"]:
        print(result["name"], result["_links"]["self"]["href"])


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
        temp = temp.drop(columns=['level'])
        df_list.append(temp)
    df_wide = df_list[0].copy()

    for i in range(0, len(df_list)-1):
        this_lvl = i + 1
        child_lvl = i + 2

        df_wide = pd.merge(
             df_wide,
             df_list[i+1],
             how='left',
             left_on=f'code_{this_lvl}',
             right_on=f'parentCode_{child_lvl}'
        )
    return df_wide