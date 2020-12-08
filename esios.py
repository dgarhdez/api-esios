import pandas as pd
import requests
import datetime
import time
from functools import reduce


class Esios:
    """
    Creates class with which to download data from esios API (Spanish TSO)
    """

    def __init__(self, token):
        """
        Initializes the object with the token and creates the header for requesting info to esios
        :param token: string to get access to data (get one at https://www.esios.ree.es/en/page/api)
        """

        self.header = {
            'Accept': 'application/json; application/vnd.esios-api-v1+json',
            'Content-Type': 'application/json',
            'Host': 'api.esios.ree.es',
            'Authorization': f'Token token="{token}"',
            'Cookie': ''
        }

    @staticmethod
    def _convert_to_df(dfs_list: dict):
        """
        converts dictionary of dataframes into a single, merged dataframe
        """
        # renames columns and appends df to list
        dfs = []
        for dict_ind in dfs_list:
            df = dict_ind["dataframe"]
            ind = dict_ind["indicator"]
            name = dict_ind["name"]
            df.rename(
                columns={"value": f"{ind}_{name}"},
                inplace=True
            )
            dfs.append(df)

        return reduce(lambda left, right: pd.merge(left, right, on="datetime"), dfs)

    def get_indicator(
        self,
        name: str,
        indicator: int,
        start_date: datetime.datetime,
        end_date: datetime.datetime
    ):
        """
        Gets data as pd.DataFrame between the chosen dates and for the chosen indicators
        :param indicator:
        :param start_date:
        :param end_date:
        :return:
        """

        param = {'start_date': start_date, 'end_date': end_date, 'time_trunc': 'hour'}
        url = 'https://api.esios.ree.es/indicators/' + str(indicator)

        raw_data = requests.get(
            url,
            headers=self.header,
            params=param
        )
        data_to_json = raw_data.json()['indicator']['values']

        df_raw = pd.DataFrame(data_to_json)

        if len(df_raw["geo_name"].unique()) > 1:
            if "España" in df_raw["geo_name"].unique():
                df_raw = df_raw.query("geo_name == 'España'").reset_index(drop=True)
            else:
                df_raw = (
                    df_raw
                    .groupby("datetime")
                    .sum()
                    .reset_index()
                )

        return df_raw[["datetime", "value"]]

    def get_several_indicators(
        self,
        indicators_dict: dict,
        start_date: datetime,
        end_date: datetime
    ):
        """
        Returns information for several indicators if they have similar structure i.e. granularity,
        country, region, etc
        :param indicators_list:
        :param start_date:
        :param end_date:
        :return:
        """

        dfs_list = []

        for name, indicator in indicators_dict.items():
            
            t0 = time.time()
            df = Esios.get_indicator(self, name, indicator, start_date, end_date)
            t1 = time.time()
            print(f"Downloaded indicator `{name}`: {t1 - t0:.0f} s ")
            
            dfs_list.append({
                "name": name,
                "indicator": indicator,
                "dataframe": df,
            })

        return self._convert_to_df(dfs_list)
