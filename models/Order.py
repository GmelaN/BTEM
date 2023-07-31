from typing import Literal, Dict
from RequestManager.RequestManager import RequestManager

import hashlib
from urllib.parse import unquote, urlencode

import uuid


class Order():
    def __init__(self):
        self.requestManager = RequestManager()

    def order(
            self,
            order_type: Literal["bid", "ask"],
            volume: float=0.01,
            method: Literal["market_price"]|float="market_price"
            ) -> str:
        '''
        upbit API를 활용하여 주문을 넣습니다.

        Args
        ----
        order_type: Literal["bid", "ask"]
            - "bid"인 경우 매수. "ask"인 경우 매도 주문입니다.

        volume: float=0.01
            - 주문량을 지정합니다.

        method: Literal["market_price"] | float="market_price"
            - 주문 가격을 지정합니다.
            - "market_price"인 경우 시장가로 지정합니다.
            - 특정 값이 들어간 경우 지정가로 주문을 진행합니다.
        '''

        raise NotImplementedError("currently creating order is not supported.")

        # 시장가 주문
        if method == "market_price":
            # 매수
            if order_type == "bid":
                ord_type = "price"

            # 매도
            elif order_type == "ask":
                ord_type = "market"

            else:
                raise ValueError(f"failed to parse order_type: {order_type}.")

        # 지정가 주문
        elif type(method) is float:
            ord_type = "limit"

        else:
            raise ValueError(f"failed to parse order_type: {order_type}.")


        url = self.requestManager.generate_url(source="upbit", api_url="")

        params = {
            'market': 'KRW-BTC',
            'side': order_type,
            'ord_type': ord_type,
            'price': method if type(method) is float else None,

        }

        header = self.requestManager.generate_header(source="upbit", payload=dict())        
        return "not implemented"


    def order_available(self) -> Dict[str, str]:
        '''
        주문 가능 여부 정보를 가져옵니다.
        여기에는 수수료 정보도 포함됩니다.

        Args
        ----
        None

        Raises
        ------
        RuntimeError: 요청에 대한 서버의 응답이 200이 아닐 떄 발생합니다.

        Returns
        -------
        response: Dict[str, str], 주문 가능 여부 정보가 담긴 딕셔너리입니다.
        '''
        url = self.requestManager.generate_url(
            source="upbit",
            api_url="v1/orders/chance",

            )

        params = {
            'market': 'KRW-BTC'
        }

        payload = {
            'nonce': str(uuid.uuid4()),
            'query_hash': self._encode_queries(params=params, encode_url=True, hash_alg="SHA512"),
            'query_hash_alg': "SHA512",
        }

        header = self.requestManager.generate_header(source="upbit", payload=payload)
        response = self.requestManager.delayed_get(url=url, headers=header)

        if response.status_code != 200:
            raise RuntimeError(f"server returned response code: {response.status_code}, reason: {response.reason}")

        return dict(response.json())


    def _encode_queries(self, params: dict[str, str], hash_alg: Literal["SHA512"]="SHA512") -> str:
        '''
        upbit API에서 요구하는 파라미터 해시화를 진행하는 메서드입니다.

        Args
        ----
        params: Dict[str, str]
            - 해시화를 진행할 파라미터
        
        hash_alg: Literal["SHA512"]="SHA512"
            - 해시 함수를 지정합니다.
        

        '''

        if hash_alg != "SHA512":
            raise NotImplementedError(f"hash alg {hash_alg} is not supported, only SHA512 is supported for now.")

        query_string = unquote(urlencode(params, doseq=True)).encode("utf-8")

        m = hashlib.sha512()
        m.update(query_string)
        query_hash = m.hexdigest()

        return query_hash
