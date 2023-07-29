from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from typing import Dict, Literal


class Duration():
    '''
    BTEM의 날짜 및 시간 클래스입니다.
    
    생성자로 전달되는 interval 정보가 반영된 iterator가 내장되어 iterable 조건을 만족합니다.

    
    예제
    ----

    >> duration = Duration(start="2020-01-01T00:00", interval="MONTH", end="2021-01-02T00:00", batch_size=1)
    >> for batch_start, batch_end in duration:
    ...     print(batch_start, batch_end)
    ...
    2020-01-01 00:00:00 2020-02-01 00:00:00
    2020-03-01 00:00:00 2020-04-01 00:00:00
    2020-05-01 00:00:00 2020-06-01 00:00:00
    2020-07-01 00:00:00 2020-08-01 00:00:00
    2020-09-01 00:00:00 2020-10-01 00:00:00
    2020-11-01 00:00:00 2020-12-01 00:00:00
    2021-01-01 00:00:00 2021-01-02 00:00:00
    '''


    def __init__(
            self,
            start: str,
            end: str,
            batch_size: int=1,
            interval: Literal["DAY", "HOUR", "MONTH"]="DAY",
            _format: str="%Y-%m-%dT%H:%M",
            return_str_in_iter: bool=True,
        ) -> None:
        '''
        Args
        ----
        start: str, 시작 일시를 표현한 문자열
        end: str, 종료 일시를 표현한 문자열
        batch_size: int=1, (iterable 객체로 사용 시) 배치 크기
        interval: Literal["DAY", "HOUR", "MONTH"]="DAY", 시점과 종점 간 간격을 지정
        _format: str="%Y%m%d", start와 end의 표현 형식

        Examples
        --------
        1일 간격으로 2010년 1월 1일부터 2020년 1월 1일까지를 나타내는 Duration
        >> Duration(start="20100101", end="20200101", interval="DAY")
        '''

        # 파라미터 검증
        if batch_size <= 0:
            raise ValueError("batch size must over 0.")
        
        if interval not in ("DAY", "HOUR", "MONTH"):
            raise ValueError("invaild interval vallue found.")


        # datetime 객체로 변환
        start_dt, end_dt = datetime(2020, 1, 1), datetime(2020, 1, 1)

        try:
            start_dt = datetime.strptime(start, _format)
            end_dt = datetime.strptime(end, _format)
        except:
            print("format and date string must be match.")

        # start, end 간의 시간 순서 유효성 검사
        if (end_dt - start_dt) < timedelta(days=0, hours=0, seconds=0):
            raise ValueError(f"start date must earier than end date, got start: {start}, end: {end}")


        # 간격 지정
        if interval == "MONTH":
            self.period_id = "1MTH"
            interval_dt = relativedelta(months=1)
        elif interval == "HOUR":
            self.period_id = "1HRS"
            interval_dt = timedelta(hours=1)
        else:
            self.period_id = "1DAY"
            interval_dt = timedelta(days=1)

        # 객체 멤버에 저장
        self.start = start_dt
        self.end = end_dt
        self.interval = interval_dt
        self.current = start_dt
        self.batch_size = batch_size
        self.return_str_in_iter = return_str_in_iter


    def strftime(self, timezone=False) -> Dict[str, str]:
        '''
        저장된 날짜 데이터를 문자열로 변환하여 반환합니다.
        
        Args
        ----
        timezone: bool=False, 반환할 날짜 데이터에 timezone을 포함할지 여부를 결정합니다.

        
        Raises
        ------
        None

        Returns
        -------
        Dict["start": str, "end": str], 변환된 데이터를 딕셔너리 형태로 반환합니다.
        '''


        start, end = self.start.isoformat().split('.')[0], self.end.isoformat().split('.')[0],

        if timezone:
            start += "+09:00"
            end += "+09:00"        

        result = {"start": start, "end": end}
        return result


    # iteration 구현 코드
    def __iter__(self):
        return self


    # iteration 구현 코드
    def __next__(self):
        # 각 반복마다 각 배치 크기만큼 반환
        if self.current < self.end:
            batch_start = self.current
            self.current += (self.interval * (self.batch_size + 1))

            batch_end = batch_start + self.interval * self.batch_size

            if batch_end > self.end:
                batch_end = self.end
            
            if self.return_str_in_iter:
                return (batch_start.isoformat().split('.')[0] + "+09:00", batch_end.isoformat().split('.')[0] + "+09:00")
            
            return (batch_start, batch_end)

        self.current = self.start
        raise StopIteration


if __name__ == "__main__":
    duration = Duration(start="2023-07-01T00:00", end="2023-07-25T00:00", batch_size=50, interval="HOUR", return_str_in_iter=True)
    for i, j in duration:
        print(i, j)
