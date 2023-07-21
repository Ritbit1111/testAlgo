class FetchToken:
    def __init__(self, logger, api) -> None:
        self.logger = logger
        self.api = api

    def get_tsym_token(self, cname):
        tsym=[]
        token=[]
        for cn in cname:
            obj = self.api.searchscrip(exchange='NSE', searchtext=cn)
            found=False
            for i in obj['values']:
                if (i['tsym'].endswith('EQ') and i['cname']==cn):
                    tsym.append(i['tsym'])
                    token.append(i['token'])
                    found=True
            if not found: 
                self.logger.error("Unable to find Token for %s", cn) 
        return tsym, token