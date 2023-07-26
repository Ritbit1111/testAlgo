from click import clear
import os
import datetime
from src.api.noren import NorenApiPy
from src.connectFlattrade import initialize
import dotenv
from src.logger import get_logger
from src.DataFetcher import FTDataService
import pandas as pd

dotenv.load_dotenv()
logger = get_logger(filename="./log")
today = datetime.date.today().strftime("%d-%m-%Y")
api = initialize(today, logger)

ft_data = FTDataService(logger=logger, api=api, path='./apidata')
ans  = ft_data.get_token_single(exchange="NFO", trading_symbol="RELIANCE27JUL23F")
print(ans)