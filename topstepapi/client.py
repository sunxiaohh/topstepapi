from .auth import AuthClient
from .account import AccountAPI
from .contract import ContractAPI
from .order import OrderAPI
from .position import PositionAPI
from .trade import TradeAPI
from .history import HistoryAPI
from .realtime import RealTimeClient
from .marketdata import MarketDataClient

import time
import threading

class TopstepClient:
    def __init__(self, username: str, api_key: str, base_url: str = "https://api.thefuturesdesk.projectx.com"):
        self.base_url = base_url
        self.auth = AuthClient(base_url)

        self._username = username
        self._api_key = api_key

        self._lock = threading.RLock()
        self._token = None
        self._token_ts = 0.0

        # 你可以按经验设置 20-23 小时刷新一次，留安全缓冲
        self.token_ttl_seconds = 23 * 3600

        self.auth.login(username, api_key)
        self._set_token(self.auth.get_token())

        # realtime/market_data 原本就带 token_provider，保留
        self.realtime = RealTimeClient(self._token, token_provider=self.auth.get_token)
        self.market_data = MarketDataClient(self._token, token_provider=self.auth.get_token)

    def _set_token(self, token: str):
        self._token = token
        self._token_ts = time.time()
        # 重建所有 REST API 子对象
        self.account = AccountAPI(self._token, self.base_url)
        self.contract = ContractAPI(self._token, self.base_url)
        self.order = OrderAPI(self._token, self.base_url)
        self.position = PositionAPI(self._token, self.base_url)
        self.trade = TradeAPI(self._token, self.base_url)
        self.history = HistoryAPI(self._token, self.base_url)

    def refresh_if_needed(self, force: bool = False) -> str:
        with self._lock:
            now = time.time()
            expired = (now - self._token_ts) >= self.token_ttl_seconds

            if force or expired or not self._token:
                # 保险起见：如果 auth 内部 token 失效，重新 login
                try:
                    new_token = self.auth.get_token()
                except Exception:
                    self.auth.login(self._username, self._api_key)
                    new_token = self.auth.get_token()

                if new_token and new_token != self._token:
                    self._set_token(new_token)

            return self._token

    @property
    def token(self):
        # 任何地方读 token 都会触发按需刷新
        return self.refresh_if_needed()