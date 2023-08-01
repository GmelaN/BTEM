from collections import namedtuple
from typing import Iterable, Literal

import urllib.parse as url_parser
from urllib.parse import urlencode, unquote

import requests
from random import random
from time import sleep

import json
from jwt import jwk, JWT

import hashlib

class RequestManager():
    def __init__(self):
        self.UrlComponents = namedtuple(typename="UrlComponents", field_names=["scheme", "netloc", "url", "params", "query", "fragment"])

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

    
    def generate_url(self, source: Literal["upbit", "coinapi"], api_url: str, query: dict | None=None):
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


        url_params = self.UrlComponents(
            scheme = "https",
            netloc = URL_SOURCES[source],
            url = api_url,
            params = "",
            query = query_,
            fragment = "",
        )

        result_url = str(url_parser.urlunparse(url_params))
        return result_url
    

    def generate_header(self, source: Literal["upbit", "coinapi"], payload: dict|None=None):
        """
        API 호출을 위한 header를 생성하는 메서드입니다.

        generates header for calling API.
        
        Parameters
        ----------
        source : Literal["upbit", "coinapi"]
            호출할 API의 소스를 지정합니다.

        payload: dict|None=None
            헤더에 탑재할 추가 파라미터를 지정합니다.
             
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


    def delayed_get(
            self,
            url: str,
            headers: dict,
            sleep_time: int|float=0.1,
            random_range: int=1,
            _raise_on_error: bool=True,
            ) -> requests.Response:
        '''
        서버의 과부하와 이용 차단을 막기 위해, (지정된 시간 + 임의 시간)동안 딜레이하여 get 요청을 보냅니다.

        Parameters
        ----------
        url: str, 요청을 보낼 URL
        headers: dict, 요청을 보낼 떄 사용할 header
        sleep_time: int|float=0.1, 최소 딜레이 시간
        random_range: int=1, sleep_time에 추가할 임의 시간의 범위

        Returns
        -------
        requests.Response: get request의 결과
        '''

        # 잘못된 범위의 값이 들어온 경우
        if sleep_time < 0 or random_range < 0:
            raise ValueError("sleep time, random_range must be 0 or upper.")

        # 딜레이 시간이 지정되지 않은 경우        
        if sleep_time == 0 and random_range == 0:
            print("warning: request will sent with no delay time.")

        sleep(sleep_time + random() * random_range)

        response = requests.get(url, headers=headers)

        if response.status_code != 200 and _raise_on_error:
            raise RuntimeError((
                "server responsed with error code: "
                f"{response.status_code}, "
                f"reason is: {response.reason}"
            ))

        return response


    def get(self, url: str, headers: dict, _raise_on_error: bool=True, *kwargs) -> requests.Response:
        response = requests.get(url, headers=headers, *kwargs)

        if response.status_code != 200 and _raise_on_error:
            raise RuntimeError((
                "server responsed with error code: "
                f"{response.status_code}, "
                f"reason is: {response.reason}"
            ))

        return response
