import os
import pandas as pd
import datetime

class FnOEquityNSE:
    NSE_URL = "https://archives.nseindia.com/content/fo/fo_mktlots.csv"
    FO_MKTLOTS = 'fo_mktlots.csv'
    FO_MKTLOTS_EDITED = 'fo_mktlots_edited.csv'

    def __init__(self, logger, path='../nsedata/fo', df_nse_raw=None):
        self.path = path
        self.logger = logger
        self.df_nse_raw = df_nse_raw
        self.fo_raw = os.path.join(self.path, self.FO_MKTLOTS)
        self.fo_edited = os.path.join(self.path, self.FO_MKTLOTS_EDITED)

    def get_latest(self, update=False):
        if update:
            self.update_latest()
        try:
            df = pd.read_csv(self.fo_edited)
        except:
            df = self.create_latest()
        return df

    def update_latest(self):
        df = self.create_latest()
        self.logger.info("Updating edited F&O equity list")
        df.to_csv(self.fo_edited, index=False)

    def create_latest(self):
        self.update_raw()
        df = self.fetch_raw_from_storage()
        df_edited = self.edit_raw(df)
        return df_edited

    def fetch_from_nse(self):
        self.logger.info("Getting raw F&O equity list from NSE")
        return pd.read_csv(self.NSE_URL)
    
    def fetch_raw_from_storage(self):
        self.logger.info("Fetching raw F&O equity list data from local storage")
        return pd.read_csv(self.fo_raw)

    def update_raw(self):
        df = self.fetch_from_nse()
        self.logger.info("Updating raw F&O equity list data to local storage")
        df.to_csv(self.fo_raw, index=False)

    def edit_raw(self, df):
        self.logger.info("Editing raw NSE F&O euity list data")
        MMM_YY = datetime.date.today().strftime("%b-%y").upper()
        # Remove beginning and trailing spaces from columns name
        df.columns = [i.strip() for i in list(df.columns)]
        df_obj = df.select_dtypes(['object'])
        df[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
        # Get rid of the NIFTY indexes
        df_equity = df[self._get_equity_index(df):]
        # df_equity = df_equity[['UNDERLYING', 'SYMBOL', f'{MMM_YY}']]
        return df_equity

    def _get_equity_index(self, df):
        for i, b in df['SYMBOL'].str.contains('Symbol').items():
            if b:
                return i+1
        mssg = "Unable to find equity position in the dataframe!"
        self.logger.error(mssg)
        raise Exception(mssg)
    
if __name__=="__main__":
    fnoconnect = FnOEquityNSE()
    df = fnoconnect.get_latest(update=True)