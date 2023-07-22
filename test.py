import pandas as pd
import datetime
from src.logger import get_logger
from src.FnOList import FnOEquityNSE
from src.connectFlattrade import ConnectFlatTrade
from src.noren import NorenApiPy
from src.EquityToken import FetchToken
import dotenv
import os

dotenv.load_dotenv()
logger = get_logger(filename='./log')


# Init API
genToken = False
if genToken:
  con = ConnectFlatTrade(logger=logger)
  resp = con.run()
  con.set_token_to_dotenv()
token = os.getenv('TODAYSTOKEN')
print(token)
api = NorenApiPy()
usersession=token
userid = 'FT020770'
ret = api.set_session(userid= userid, password = '', usertoken= usersession)

# Get fno equity df
is_present=True
if not is_present:
  fnoconnect = FnOEquityNSE(logger=logger, path='./nsedata/fo')
  df = fnoconnect.get_latest(update=True)
  print(df.shape)

  def add_token():
    token_api = FetchToken(logger, api)
    tsym, token = token_api.get_tsym_token(df['UNDERLYING'])
    df['tsym'] = tsym
    df['token'] = token
    df.to_csv('./apidata/fno_equity_tsym_token.csv', index=False)
  add_token()

df_tsym = pd.read_csv('./apidata/fno_equity_tsym_token.csv')