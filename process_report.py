import os
import datetime
import pandas as pd
import dotenv
from src.connectFlattrade import initialize
from src.logger import get_logger

dotenv.load_dotenv()
logger = get_logger(filename="./log")
today = datetime.datetime(2023, 7, 28)
today_str = today.strftime("%d-%m-%Y")
# api = initialize(today_str, logger)

strat_momentum_path = os.path.join("apidata", "momentum", today_str)
os.makedirs(strat_momentum_path, exist_ok=True)

# ft_data = FTDataService(logger=logger, api=api, path="./apidata")

report_path = os.path.join(strat_momentum_path, f'report_{today_str}.csv')
report_path_prettify = os.path.join(strat_momentum_path, f'report_{today_str}_cleaned.csv')
df = pd.read_csv(report_path, usecols=['ordtime', 'sym', 'tsym', 'token', 'instrument', 'lotsize', 'avgprice', 'qty', 'tranType', 'totalprice'])
print(df.columns)
print(df)
df.to_csv(report_path_prettify)