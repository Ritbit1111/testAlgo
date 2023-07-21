import os
import hashlib
from urllib.parse import parse_qs, urlparse
import requests
import pyotp
import dotenv


class ConnectFlatTrade:
    sessionid_header ={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/112.0.0.0 Safari/537.36","Referer":"https://auth.flattrade.in/"} 
    sessionid_url = 'https://authapi.flattrade.in/auth/session' 
    request_code_url = 'https://authapi.flattrade.in/ftauth' 
    token_url = 'https://authapi.flattrade.in/trade/apitoken' 

    def __init__(self, logger, dotenv_path=None):
        self.dotenv_path = dotenv_path
        if self.dotenv_path is None:
            self.dotenv_path = dotenv.find_dotenv()
        dotenv.load_dotenv(dotenv_path=dotenv_path, override=True)
        self.session_id = None
        self.request_code = None
        self.usertoken = None
        self.ses = requests.Session() 
        self.logger = logger

    def run(self):
        self.logger.info("Starting the token generation!")
        self.session_id = self.get_session_id()
        self.request_code = self.get_request_code()
        self.usertoken = self.get_token()
        self.logger.info("Success: Token generated")
        return {
            "SESSION_ID":self.session_id,
            "REQUEST_CODE":self.request_code,
            "USER_TOKEN":self.usertoken,
        }
    
    def set_token_to_dotenv(self):
        self.logger.info("Setting the TODAYSTOKEN to the .env")
        dotenv.set_key(self.dotenv_path, "TODAYSTOKEN", self.usertoken, quote_mode="auto")
        dotenv.load_dotenv(dotenv_path=self.dotenv_path, override=True)

    def get_session_id(self):
        sessionid_response = self.ses.post(self.sessionid_url,headers=self.sessionid_header) 
        return sessionid_response.text 

    def get_request_code(self):
        totp = pyotp.TOTP(os.getenv("TOTPKEY")) 
        passwordEncrpted =  hashlib.sha256(os.getenv("PASSWORD").encode()).hexdigest() 
        payload = {"UserName":os.getenv("USERID"),"Password":passwordEncrpted,"PAN_DOB":totp.now(),"App":"","ClientID":"","Key":"","APIKey":os.getenv("APIKEY"),"Sid":self.session_id} 
        code_response = self.ses.post(self.request_code_url, json=payload) 
        reqcodeRes = code_response.json() 
        parsed = urlparse(reqcodeRes['RedirectURL']) 
        return parse_qs(parsed.query)['code'][0] 

    def get_token(self):
        api_secret = os.getenv("APIKEY")+ self.request_code + os.getenv("SECRETKEY") 
        api_secret =  hashlib.sha256(api_secret.encode()).hexdigest() 
        payload = {"api_key":os.getenv("APIKEY"), "request_code":self.request_code, "api_secret":api_secret} 
        token_response = self.ses.post(self.token_url, json=payload) 
        usertoken = token_response.json()['token'] 
        return usertoken

if __name__=="__main__":
    con = ConnectFlatTrade()
    con.run()
    con.set_token_to_dotenv()