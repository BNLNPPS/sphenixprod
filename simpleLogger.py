import logging

# Consider https://signoz.io/guides/how-should-i-log-while-using-multiprocessing-in-python/
# for multiprocessing logging and/or buffered logging for I/O performance.

# ============================================================================
# prettier logging for console output
class CustomFormatter(logging.Formatter):
    grey     = "\x1b[38;20m"
    yellow   = "\x1b[33;20m"
    green    = "\x1b[32;20m"
    blue     = "\x1b[34;20m"    
    red      = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset    = "\x1b[0m"
    # format   = "%(asctime)s - %(name)s - %(levelname)s - %(message)s (%(filename)s:%(lineno)d)"
    format   = "%(asctime)s [%(levelname)s] - %(message)s"
    # format   = "%(asctime)s [%(levelname)s] - %(message)s (%(filename)s:%(lineno)d)"

    FORMATS = {
        logging.WARNING:  blue     + format + " (%(filename)s:%(lineno)d) " + reset,
        logging.DEBUG:    grey     + format + " (%(filename)s:%(lineno)d) " + reset,
        logging.INFO:     green    + format + reset,
        logging.ERROR:    red      + format + " (%(filename)s:%(lineno)d) " + reset,
        logging.CRITICAL: bold_red + format + " (%(filename)s:%(lineno)d) " + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)
                                                                
# ============================================================================
# def slogger ( filename = None, name = 'sphenixprod'):
slogger = logging.getLogger( 'sphenixprod' )
ch = logging.StreamHandler()
ch.setFormatter(CustomFormatter())
slogger.addHandler(ch)

DEBUG    = slogger.debug
INFO     = slogger.info
WARN     = slogger.warning
ERROR    = slogger.error
CRITICAL = slogger.critical

