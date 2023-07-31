import pytz
import datetime

def get_epoch_time(time_string):
    indian_tz = pytz.timezone('Asia/Kolkata')
    naive  = datetime.datetime.strptime(time_string, '%d-%m-%Y %H:%M:%S')
    ind = indian_tz.localize(naive)
    return ind.timestamp()

def epochIndian(datetimeobj:datetime.datetime):
    indian_tz = pytz.timezone('Asia/Kolkata')
    naive  = datetimeobj.replace(tzinfo=None)
    ind = indian_tz.localize(naive)
    return ind.timestamp()

def get_datetime(epochsecs:float)->datetime.datetime:
    utc = pytz.utc
    utc_dt = datetime.datetime.utcfromtimestamp(epochsecs).replace(tzinfo=utc)
    indian_tz = pytz.timezone('Asia/Kolkata')
    return utc_dt.astimezone(indian_tz)

if __name__=="__main__":
    assert get_epoch_time("27-07-2023 09:15:00") == 1690429500
    dtim = datetime.datetime.strptime("27-07-2023 09:15:00", '%d-%m-%Y %H:%M:%S')
    assert epochIndian(dtim) == 1690429500
    print(get_datetime(1690429500))