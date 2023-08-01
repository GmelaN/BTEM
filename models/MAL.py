from DataFetcher.DataFetcher import DataFetcher
from DataFetcher.Duration import Duration

from typing import Iterable
import pandas as pd
from datetime import timedelta
from dateutil.parser import isoparse

class MAL_model():
    '''
    이동평균선을 기반으로 한 예측 모델입니다.

    
    '''
    def __init__(
            self,
            data: pd.DataFrame,
            duration: Duration,
            moving_avr_interval_days: Iterable[int]=[5, 20, 60, 120, 240],
            target_label: str="price_close",
            timestamp_label: str="time_period_start",
            ) -> None:
        '''
        MAL_model 인스턴스를 생성합니다.

        Args
        ----
        data: pd.DataFrame, 데이터
        duration: Duration, 데이터의 시간 간격을 담은 Duration 객체
        moving_avr_interval_days: Iterable[int]=[5, 10, 60, 120, 240], 구할 이동평균의 간격들
        target_label: str="price_close", 이동평균을 구할 라벨
        timestamp_label: str="time_period_start", 데이터의 타임스탬프 라벨

        Raises
        ------
        ValueError: 데이터셋에 라벨이 존재하지 않거나 충분하지 않은 길이의 데이터셋인 경우 발생합니다.

        Returns
        -------
        None
        '''

        # 데이터셋의 시간 간격 파악
        if duration.interval == timedelta(hours=1):
            self.days_weight = 24
        elif duration.interval == timedelta(days=1):
            self.days_weight = 1
        else:
            raise ValueError("데이터셋의 시간 단위가 1HOUR, 1DAY인 경우에만 사용할 수 있습니다.")


        # 데이터셋의 최대 시간 간격 파악
        max_interval = duration.end - duration.start
        moving_avr_interval_days = [day for day in moving_avr_interval_days if timedelta(days=day) < max_interval]

        if len(moving_avr_interval_days) == 0:
            raise ValueError("최소 5일 동안의 데이터가 있어야 이동평균선을 구할 수 있습니다.")
        

        # 타깃 라벨이 데이터셋에 있는지 파악
        if target_label not in data.keys():
            raise ValueError(f"{target_label}이 데이터셋에 없습니다.")
        
        if timestamp_label not in data.keys():
            raise ValueError(f"{timestamp_label}이 데이터셋에 없습니다.")

        self.moving_avr_interval_days = moving_avr_interval_days
        self.data = data.sort_values(by=timestamp_label)    # 시간순 오름차순 정렬
        self.target_label = target_label
        self.timestamp_label = timestamp_label

        return




    def add_mal(self, inplace: bool=False) -> pd.DataFrame:
        '''
        이동평균 데이터를 구합니다.

        Args
        ----
        inplace: bool=False, 구한 데이터를 객체 내부 데이터셋에 덮어쓸지를 결정합니다.

        Raises
        ------
        None

        Returns
        -------
        data: pd.DataFrame, 이동 평균이 추가된 데이터셋
        '''


        # 이동평균선 구하기
        if inplace:
            data = self.data
        else:
            data = self.data.copy()


        for day in self.moving_avr_interval_days:
            moving_avr = data[self.target_label].rolling(window=day * self.days_weight).mean()
            data[f"MAL_{day}DAY"] = moving_avr

        return data
    

    def predict(self, target_time: str|None=None, MAL_short: str="MAL_5DAY", MAL_long: str="MAL_20DAY", cross_duration: int=3) -> bool:
        '''
        장기 이동 평균과 단기 이동 평균의 교차 여부를 기반으로 매수 여부를 추측합니다.

        Args
        ----
        target_time: str | None=None, 예측의 대상인 시점을 선택합니다.
        MAL_short: str="MAL_5DAY", 단기 이동 평균을 선택합니다.
        MAL_short: str="MAL_20DAY", 장기 이동 평균을 선택합니다.
        cross_duration: int=3, 매수 여부 추측을 위해, 단기 이동 평균이 장기 이동 평균보다 몇일동안 더 커야 하는지를 결정합니다.

        Raises
        ------
        ValueError: 데이터셋에 라벨이 존재하지 않는 경우 발생합니다.

        Returns
        -------
        prediction: bool, 매수 여부, True인 경우 매수 신호입니다.
        
        '''


        # 라벨 검증
        if MAL_short not in self.data.keys() or MAL_long not in self.data.keys():
            raise ValueError(f"{MAL_short} 또는 {MAL_long}이 데이터셋에 없습니다. add_mal(inplace=True)로 데이터를 먼저 추가하세요.")
        
        # 예측 시점의 인덱스 구하기
        if target_time is not None:
            if not (self.data[self.timestamp_label] == target_time).any():
                raise ValueError(f"{target_time}이 타임스탬프 라벨 {self.timestamp_label}에 없습니다.")

            index = self.data[self.data[self.timestamp_label] == target_time].index[0]

            if index - cross_duration < 0:
                raise ValueError(f"{target_time} 이전 데이터가 {cross_duration}개보다 적습니다.")
        
        # 시점이 선택되지 않았다면 가장 최근으로 설정
        else:
            index = len(self.data)

        duration_index = self.days_weight * cross_duration - 1
        prediction = (self.data[MAL_short][index - duration_index : index] > self.data[MAL_long][index - duration_index : index]).mode()

        # 같은 빈도를 가진다면 False로 예측
        if len(prediction) != 1:
            return False
        
        else:
            return prediction[0]
