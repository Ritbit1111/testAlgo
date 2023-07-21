import pandas as pd
import datetime
from src.logger import get_logger
from src.FnOList import FnOEquityNSE
from src.connectFlattrade import ConnectFlatTrade
from src.noren import NorenApiPy
import dotenv
import os

dotenv.load_dotenv()
logger = get_logger(filename='./log')

genToken = False

if genToken:
  con = ConnectFlatTrade(logger=logger)
  resp = con.run()
  con.set_token_to_dotenv()
# print(resp)
# token = resp['USER_TOKEN']
token = os.getenv('TODAYSTOKEN')
print(token)

fnoconnect = FnOEquityNSE(logger=logger, path='./nsedata/fo')
df = fnoconnect.get_latest(update=False)
print(df.head())


api = NorenApiPy()
usersession=token
userid = 'FT020770'

ret = api.set_session(userid= userid, password = '', usertoken= usersession)
ret = api.get_limits()
print(ret)

for index, row in df.iterrows():
  st = row['UNDERLYING']
  print(st)
  obj = api.searchscrip(exchange='NSE', searchtext=st)
  for i in obj['values']:
    if (i['tsym'].endswith('EQ')):
      print(i['tsym'], i['token'])
      row['tsym'] = i['tsym']
      row['token'] = i['token']
  if index>2:
    break
  print('-----------------------------')
# df.to_csv('/content/fo_scrips.csv')