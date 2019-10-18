import pandas as pd
import requests
import datetime
from functools import reduce


class Esios:
    """
    Creates class with which download data from esios API (Spanish TSO)
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

        self.datetime_fmt = "%Y-%m-%d %H:%M%:%S"
        self.geo_ids = [3, 8741]

    @staticmethod
    def convert_to_df(dict_dfs: dict) -> pd.DataFrame:
        """
        converts dictionary of dataframes into a single, merged dataframe
        """
        # renames columns to include the indicator number in the columns
        for ind, df in dict_dfs.items():
            df.rename(columns={"value": f"ind_{ind}"}, inplace=True)

        dfs = [df[["datetime"] + [col for col in df.columns if "ind" in col]] for df in
               dict_dfs.values()]

        return reduce(lambda left, right: pd.merge(left, right, on="datetime"), dfs)

    def get_indicator(self,
                      indicator: int,
                      start_date: datetime.datetime,
                      end_date: datetime.datetime) -> pd.DataFrame:
        """
        Gets data as pd.DataFrame between the chosen dates and for the chosen indicators
        :param indicator:
        :param start_date:
        :param end_date:
        :return:
        """

        param = {'start_date': start_date, 'end_date': end_date, 'time_trunc': 'hour'}
        url = 'https://api.esios.ree.es/indicators/' + str(indicator)

        raw_data = requests.get(url,
                                headers=self.header,
                                params=param)
        data_to_json = raw_data.json()['indicator']['values']

        df_raw = pd.DataFrame(data_to_json)
        df_raw.rename(columns={"value": f"ind_{indicator}"}, inplace=True)

        if len(df_raw["geo_name"].unique()) > 1:
            if "ind_600" in df_raw.columns:
                df_raw = df_raw.query("geo_name == 'España'").reset_index(drop=True)
            else:
                df_raw = (
                    df_raw
                    .groupby("datetime")
                    .sum()
                    .reset_index()
                )

        return df_raw[["datetime"] + [col for col in df_raw.columns if "ind" in col]]

    def get_several_indicators(self,
                               indicators_list: list,
                               start_date: datetime,
                               end_date: datetime) -> dict:
        """
        Returns information for several indicators if they have similar structure i.e. granularity,
        country, region, etc
        :param indicators_list:
        :param start_date:
        :param end_date:
        :return:
        """
        # if there's only one indicator in the list
        if len(indicators_list) == 1:

            indicator = indicators_list[0]
            df = Esios.get_indicator(self, indicator, start_date, end_date)
            inds_dict =  {indicator: df}

        else:

            # loop through indicators
            inds_dict = {}

            for indicator in indicators_list:

                df = Esios.get_indicator(self, indicator, start_date, end_date)
                inds_dict[indicator] = df

        return inds_dict
