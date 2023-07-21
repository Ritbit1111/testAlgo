import logging
import datetime, pytz

class Formatter(logging.Formatter):
    """override logging.Formatter to use an aware datetime object"""
    def converter(self, timestamp):
        dt = datetime.datetime.fromtimestamp(timestamp)
        tzinfo = pytz.timezone('Asia/Kolkata')
        return tzinfo.localize(dt)
        
    def formatTime(self, record, datefmt=None):
        dt = self.converter(record.created)
        if datefmt:
            s = dt.strftime(datefmt)
        else:
            try:
                s = dt.isoformat(timespec='milliseconds')
            except TypeError:
                s = dt.isoformat()
        return s

def get_logger(logger_name="Logger", level=logging.DEBUG, filename=None):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(level)

    # create formatter
    formatter = Formatter('%(asctime)s - %(levelname)s - %(filename)s:%(lineno)s - %(message)s', datefmt="%d-%m-%y %H:%M:%S%p")

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)

    if filename:
        fh = logging.FileHandler(filename=filename)
        fh.setFormatter(formatter)
        logger.addHandler(fh)
    return logger