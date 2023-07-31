from jwt import JWT, jwk
import hashlib
import requests
import uuid

import json
import pandas as pd

from typing import Literal, Dict

from time import sleep
from random import random
from .Duration import Duration

import FinanceDataReader as fdr

from RequestManager.RequestManager import RequestManager

class DataFetcher():
    def __init__(self):
        self.requestManager = RequestManager()
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
        header = self.requestManager.generate_header(source="coinapi")
        
        start_date, end_date = duration.strftime(timezone=True).values()
        test_end_date = (duration.start + duration.interval * 2).isoformat().split('.')[0] + "+09:00"

        period_id = duration.period_id

        # 테스트 겸 데이터 헤더 가져오기
        test_url = self.requestManager.generate_url(
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

        test_response = self.requestManager.delayed_get(url=test_url, headers=header)

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
                url = self.requestManager.generate_url(
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
                response = self.requestManager.delayed_get(url=url, headers=header)
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
                data = fdr.DataReader(symbol="BTC", start=batch_start.strftime("%Y-%m-%dT%H%M%S"), end=batch_end.strftime("%Y-%m-%dT%H%M%S")) # type: ignore

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


    def get_market_code(self):
        pass


    def get_api_rate_info(self):
        pass


if __name__ == "__main__":
    datafetcher = DataFetcher()
    duration = Duration(start="2023-07-01T00:00", end="2023-07-02T00:00", batch_size=50, interval="DAY", return_str_in_iter=False)

    # data = datafetcher.get_bitcoin_candle(duration=duration)
    data = datafetcher.get_bitcoin_cme(duration=duration)
    print(data)
