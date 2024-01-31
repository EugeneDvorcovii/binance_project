import websockets
import asyncio
import json

from config import futures_symbols, host, user, password
from db_clients.websocket_parse_db import DBClient


class ParserWebsocetClient:
    def __init__(self, window_size: int = 100):
        self.data = dict()
        self.accumulate_data = dict()
        self.data_work = dict()
        self.window_size = window_size
        self.base_url_features = "wss://fstream.binance.com:443/stream?streams="
        self.base_url_spots = "wss://stream.binance.com:443/stream?streams="
        self.db_client = DBClient(host=host, user=user, password=password)

    def accumulating_data(self, symbol: str, price: float, volume: float, timestamp: int) -> int:

        if symbol in self.accumulate_data:
            if timestamp == self.accumulate_data[symbol]["time"]:
                self.accumulate_data[symbol]["price"] = price
                self.accumulate_data[symbol]["volume"] = volume
                self.accumulate_data[symbol]["trades"] += 1
            else:
                if symbol in self.data:
                    # Save data per second in data_work
                    price = self.accumulate_data[symbol]["price"]
                    volume = self.accumulate_data[symbol]["volume"]
                    trades = self.accumulate_data[symbol]["trades"]
                    time_timestamp = self.accumulate_data[symbol]["time"]
                    result = self.db_client.set_trade(ticker_title=symbol,
                                                      timestamp=time_timestamp,
                                                      price=price,
                                                      volume=volume,
                                                      trades=trades)
                    print(result)
                    print(time_timestamp)

                    self.data[symbol]["price"].append(self.accumulate_data[symbol]["price"])
                    # self.data[symbol]["volume"].append(self.accumulate_data[symbol]["volume"])
                    # self.data[symbol]["trades"].append(self.accumulate_data[symbol]["trades"])
                    # self.data[symbol]["silence"].append(self.accumulate_data[symbol]["silence"])
                    # self.data[symbol]["time"].append(self.accumulate_data[symbol]["time"])

                    # Create new accumulate_data
                    # silence = timestamp - self.accumulate_data[symbol]["time"]
                    self.accumulate_data[symbol] = {"price": price,
                                                    "volume": volume,
                                                    "trades": 1,
                                                    # "silence": silence,
                                                    "time": timestamp,
                                                    }
                    return 1
                else:
                    # Create new symbol in data_work
                    new_data = {"price": list(),
                                "volume": list(),
                                "trades": list(),
                                # "silence": list(),
                                "time": list(),
                                "trading": False
                                }
                    self.data[symbol] = new_data
        else:
            # Create new symbol in accumulate_data
            new_data = {"price": price,
                        "volume": volume,
                        "trades": 1,
                        # "silence": 0,
                        "time": timestamp,
                        }
            self.accumulate_data[symbol] = new_data
        return 0

    def send_data_to_filters(self, index_trade: int, symbol: str, timing: int = 10):
        # timing - таймаут, с которым будет проходить подача на фильтр. Базовое значение = 10 секунд
        if symbol in self.data:
            print(f"Len data work ({symbol}): {len(self.data[symbol]['price'])}"
                  f"\nIndex trade: {index_trade}")

    async def connect(self, symbol):
        url = self.base_url_features + f"{symbol}@trade"
        # Тайминг, по которому будут передаваться данные в систему, timing = 1 - каждую секунду
        timing = 1
        async with websockets.connect(url) as ws:
            index_trade = 0
            while True:
                # try:
                    # Получение данных с Вебсокета
                    result = json.loads(await ws.recv())["data"]
                    if "e" in result and result["e"] == "trade":
                        symbol = result["s"]
                        trade_id = result["t"]
                        price = float(result["p"])
                        volume = float(result["q"])
                        timestamp = result["T"] // (1000 * timing)
                        # Accumulate data for filters
                        index_trade += self.accumulating_data(symbol=symbol, price=price, volume=volume, timestamp=timestamp)
                        # self.send_data_to_filters(index_trade=index_trade,
                        #                           symbol=symbol,
                        #                           timing=10)

    async def get_data_websocket(self):
        symbol_list = futures_symbols
        connections = [self.connect(symbol.lower()) for symbol in symbol_list]
        await asyncio.gather(*connections)

    def run(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.get_data_websocket())


app = ParserWebsocetClient()
app.run()


