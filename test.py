import pandas as pd
import datetime
from connect.logger import get_logger
from connect.FnOList import FnOEquityNSE
from connect.connectFlattrade import ConnectFlatTrade
from connect.noren import NorenApiPy
import dotenv
import os

dotenv.load_dotenv()
logger = get_logger(filename='./log')
# con = ConnectFlatTrade(logger=logger)
# resp = con.run()
# print(resp)
# token = resp['USER_TOKEN']
token = os.getenv('TODAYSTOKEN')
print(token)

fnoconnect = FnOEquityNSE(logger=logger, path='./nsedata/fo')
df = fnoconnect.get_latest(update=False)

api = NorenApiPy()

#set token and user id
#paste the token generated using the login flow described
# in LOGIN FLOW of https://pi.flattrade.in/docs
usersession=token
userid = 'FT020770'

ret = api.set_session(userid= userid, password = '', usertoken= usersession)
# ret = api.get_limits()
# print(ret)

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