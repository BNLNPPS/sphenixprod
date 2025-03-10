import logging

# Consider https://signoz.io/guides/how-should-i-log-while-using-multiprocessing-in-python/
# for multiprocessing logging and/or buffered logging for I/O performance.

# ============================================================================
# prettier logging
class CustomFormatter(logging.Formatter):
    grey     = "\x1b[38;20m"
    yellow   = "\x1b[33;20m"
    green    = "\x1b[32;20m"
    blue     = "\x1b[34;20m"    
    red      = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset    = "\x1b[0m"
    format   = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"
    #format   = "[%(name)s %(levelname)s]" + " %(message)s"

    FORMATS = {
        logging.DEBUG:    grey     + format + reset,
        logging.INFO:     grey     + format + reset,
        logging.WARNING:  yellow   + format + reset,
        logging.ERROR:    red      + format + reset,
        logging.CRITICAL: bold_red + format + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
                                                                
# ============================================================================
# Setup logging globally
logger = logging.getLogger('slurp')

logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel( logging.DEBUG )
ch.setFormatter(CustomFormatter())
logger.addHandler(ch)

DEBUG    = logger.debug
INFO     = logger.info
WARN     = logger.warning
ERROR    = logger.error
CRITICAL = logger.critical



