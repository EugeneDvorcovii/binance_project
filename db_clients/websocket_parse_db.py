import pymysql
import time
from config import host, user, password


class DBClient:
    DATABASE_NAME = "websocket_db"
    TABLES = ["trades_1s"]

    def __init__(self, host: str, user: str, password: str):
        self.con = pymysql.connect(
            host=host,
            port=3306,
            user=user,
            password=password,
            cursorclass=pymysql.cursors.DictCursor
        )
        self.create_db()
        self.create_table()

    def create_db(self, db_title: str = DATABASE_NAME):
        try:
            request = f"CREATE DATABASE IF NOT EXISTS {db_title};"
            with self.con.cursor() as cur:
                cur.execute(request)
                self.con.commit()
                return True
        except Exception:
            return False

    def create_table(self, table_title: str = TABLES[0], db_title: str = DATABASE_NAME):
        try:
            request = f"""CREATE TABLE IF NOT EXISTS {db_title}.{table_title} (trade_id int AUTO_INCREMENT,
                          ticker_title VARCHAR(20),
                          timestamp int,
                          price float,
                          volume float,
                          trades int,
                          PRIMARY KEY(trade_id),
                          UNIQUE(ticker_title, timestamp))
                          """
            with self.con.cursor() as cur:
                cur.execute(request)
                self.con.commit()
                return True
        except Exception as ex:
            print(ex)
            return False

    def set_trade(self, ticker_title: str, timestamp: int, price: float, volume: float, trades: int,
                  table_title: str = TABLES[0], db_title: str = DATABASE_NAME) -> bool:
        try:
            request = f"INSERT INTO {db_title}.{table_title} (ticker_title, timestamp, price, volume, trades) " \
                      "VALUES (%s, %s, %s, %s, %s);"
            record = [(ticker_title, timestamp, price, volume, trades)]
            with self.con.cursor() as cur:
                cur.executemany(request, record)
                self.con.commit()
            return True
        except Exception as ex:
            print(ex)
            return False

    def get_all_trade(self, table_title: str = TABLES[0], db_title: str = DATABASE_NAME):
        request = f"SELECT * FROM {db_title}.{table_title}"
        with self.con.cursor() as cur:
            cur.execute(request)
        return cur.fetchall()

    def drop_table(self, table_title: str = TABLES[0], db_title: str = DATABASE_NAME):
        request = f"DROP TABLE {db_title}.{table_title}"
        with self.con.cursor() as cur:
            cur.execute(request)


# a = DBClient(host=host, user=user, password=password)
# a.drop_table()
# a.create_table()
# result = a.set_trade(ticker_title="test_2",
#                      timestamp=2313444,
#                      price=123.2,
#                      volume=234.3,
#                      trades=324)
# print(result)



