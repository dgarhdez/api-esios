import pandas as pd
import requests
import json
import datetime


class Esios:
    """
    Creates class with which download data from esios API (Spanish TSO)
    """

    def __init__(self, token):
        """
        Initializes the object with the token and creates the header with which request info to esios
        :param token: string with wich to get access to data (get yours at https://www.esios.ree.es/en/page/api)
        """

        self.header = {
            'Accept': 'application/json; application/vnd.esios-api-v1+json',
            'Content-Type': 'application/json',
            'Host': 'api.esios.ree.es',
            'Authorization': f'Token token="{token}"',
            'Cookie': ''
        }

    def get_indicator(self,
                      indicator: int,
                      start_date: datetime.datetime,
                      end_date: datetime.datetime) -> pd.DataFrame:
        """
        Gets data as pd.DataFrame between the chosen dates and for the chosen indicators
        :param indicator_list:
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

        return df_raw
