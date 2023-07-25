from jwt import JWT, jwk
import hashlib
import requests
import uuid

from urllib.parse import urlencode, unquote
import urllib.parse as url_parser

import json
import pandas as pd

from collections import namedtuple

from typing import Literal, Dict

from time import sleep

from Duration import Duration

import FinanceDataReader as fdr

UrlComponents = namedtuple(typename="UrlComponents", field_names=["scheme", "netloc", "url", "params", "query", "fragment"])


class DataFetcher():
    def __init__(self):
        # API KEY
        file = None
        try:
            file = open("./keys.json")
            keys = json.load(file)

            self.__upbit_access = keys["upbit_access"]
            self.__upbit_secret = keys["upbit_secret"].encode("utf-8")
            self.__upbit_jwt = jwk.OctetJWK(key=self.__upbit_secret)

            self.__coinapi_access = keys["coinapi_access"]
            file.close()

        except FileNotFoundError:
            raise FileNotFoundError("Error: api key file ./keys.json not found.")
        except Exception as e:
            if file is not None:
                file.close()
            print(e)

        return


    def get_bitcoin_candle(self, duration: Duration) -> pd.DataFrame:
        '''
        비트코인 캔들 데이터(BTC-USD, coinapi)를 가져옵니다.
        Parameters
        ----------
            duration: Duration 객체


        Raises
        ------
            RuntimeError: API로부터 정상적인 응답이 오지 않은 경우
            
        Returns
        -------
            pd.DataFrame: 캔들 데이터를 담고 있는 데이터프레임
        '''

        # 한 번에 들고 올 데이터 수
        BATCH_SIZE: int = duration.batch_size

        # request 준비
        header = self._generate_header(source="coinapi")
        
        start_date, end_date = duration.strftime(timezone=True).values()
        test_end_date = (duration.start + duration.interval * 2).isoformat().split('.')[0] + "+09:00"

        period_id = duration.period_id

        # 테스트 겸 데이터 헤더 가져오기
        test_url = self._generate_url(
            source="coinapi",
            api_url="v1/ohlcv/BITSTAMP_SPOT_BTC_USD/history",
            query={
                "period_id": period_id,
                "time_start": start_date,
                "time_end": test_end_date,
                "include_empty_items": "true",
                "limit": BATCH_SIZE
                }
            )

        test_response = requests.get(test_url, headers=header)

        # 정상적인 응답이 돌아오지 않은 경우
        if test_response.status_code != 200:
            raise RuntimeError(test_response.text)

        test_resp_json = test_response.json()

        result_json = {i:[] for i in test_resp_json[0].keys()}

        try:
            # 본 데이터 수집
            for batch_start, batch_end in duration:
                print(f"fetching {batch_start} ~ {batch_end}...", end="")
                # URL 생성
                url = self._generate_url(
                    source="coinapi",
                    api_url="v1/ohlcv/BITSTAMP_SPOT_BTC_USD/history",
                    query={
                        "period_id": period_id,
                        "time_start": batch_start,
                        "time_end": batch_end,
                        "include_empty_items": "true",
                        "limit": BATCH_SIZE
                    }
                )

                # 데이터 변환
                response = requests.get(url, headers=header)
                resp_json = response.json()

                # 정상적인 응답이 돌아오지 않은 경우
                if response.status_code != 200:
                    raise RuntimeError(response.text)
                
                # 받은 데이터를 전체 데이터에 연결
                for i in resp_json:
                    for key in result_json.keys():
                        if key in i.keys():
                            result_json[key].append(i[key])
                            continue

                        # null data
                        result_json[key].append(None)

        except Exception as e:
            # 예외 발생시 그동안 처리했던 데이터는 반환
            print(e)
            return pd.DataFrame(result_json)

        return pd.DataFrame(result_json)
    

    def get_bitcoin_cme(self, duration: Duration) -> pd.DataFrame:
        '''
        비트코인 CME 선물 데이터(BTC, FinanceDataReader)를 가져옵니다.
        Parameters
        ----------
            duration: Duration 객체
            - return_str_in_iter=False여야 합니다.
            - interval="DAY"여야 합니다. 현재로서는 일별 데이터만 수집할 수 있습니다.

        Raises
        ------
            ValueError: 조건에 맞지 않는 매개변수가 감지된 경우
            
        Returns
        -------
            pd.DataFrame: 선물 데이터를 담고 있는 데이터프레임
        '''

        result = pd.DataFrame()

        if duration.period_id != "1DAY":
            raise ValueError("fetching bitcoin CME using FinanceDataReader is currently supports DAY interval only.")
        
        if duration.return_str_in_iter == True:
            raise ValueError("this method needs turn off return_str_in_iter on Duration.")

        try:
            for batch_start, batch_end in duration:
                data = fdr.DataReader(symbol="BTC", start=batch_start.strftime("%Y-%m-%dT%H%M%S"), end=batch_end.strftime("%Y-%m-%dT%H%M%S"))

                if data is None:
                    raise RuntimeError("Empty data received.")

                result = pd.concat([result, data], axis=1)

        except Exception as e:
            # 예외 발생시 그동안 처리했던 데이터는 반환
            print(e)
            return pd.DataFrame(result)
        
        return result


    def get_account_info(self):
        pass


    def get_order_available(self):
        pass


    def make_order(self):
        pass


    def get_market_code(self):
        pass


    def get_api_rate_info(self):
        pass


    def _generate_url(self, source: Literal["upbit", "coinapi"], api_url: str, query: dict | None=None):
        """
        API 호출을 위한 URL을 생성하는 메서드입니다.

        generates URL for calling API.
        
        Parameters
        ----------
        source : Literal["upbit", "coinapi"]
            호출할 API의 소스를 지정합니다.

        root_url : str
            호출할 API의 루트 url을 지정합니다.

        query : dict|None=None
            호출할 API에 들어갈 질의를 지정합니다.
        
        
        Raises
        ------
        ValueError
            부적절한 파라미터가 전달되었을 때 발생합니다.


        Returns
        -------
        result_url
            생성된 URL 문자열    
        """

        URL_SOURCES = {
            "upbit": "api.upbit.com",
            "coinapi": "rest.coinapi.io",
        }

        # 파라미터 검증
        if source not in URL_SOURCES.keys():
            raise ValueError(f'source must be "upbit" or "coinapi". input is {source}')
        
        query_ = "" if query is None else url_parser.urlencode(query)


        url_params = UrlComponents(
            scheme = "https",
            netloc = URL_SOURCES[source],
            url = api_url,
            params = "",
            query = query_,
            fragment = "",
        )

        result_url = str(url_parser.urlunparse(url_params))
        return result_url
    

    def _generate_header(self, source: Literal["upbit", "coinapi"], payload: dict|None=None):
        """
        API 호출을 위한 header를 생성하는 메서드입니다.

        generates header for calling API.
        
        Parameters
        ----------
        source : Literal["upbit", "coinapi"]
            호출할 API의 소스를 지정합니다.

        Raises
        ------
        None
             
        Returns
        -------
        header : dict
            요청 정보가 담겨 있는 dictionary입니다.
        """


        header = {}
        payload_ = dict()

        if payload is not None:
            for key in payload.keys():
                payload_[key] = payload[key]

        if source == "upbit":
            payload_["access_key"] = self.__upbit_access

            jwt_token = JWT().encode(payload_, key=self.__upbit_jwt)

            authorization = 'Bearer {}'.format(jwt_token)
            header = { 'Authorization': authorization }

        elif source == "coinapi":
            header = {'X-CoinAPI-Key' : self.__coinapi_access}

        return header


if __name__ == "__main__":
    datafetcher = DataFetcher()
    duration = Duration(start="2023-07-01T00:00", end="2023-07-25T00:00", batch_size=50, interval="DAY", return_str_in_iter=False)

    # data = datafetcher.get_bitcoin_candle(duration=duration)
    data = datafetcher.get_bitcoin_cme(duration=duration)
    print(data)
