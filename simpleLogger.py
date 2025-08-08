import logging

# Consider https://signoz.io/guides/how-should-i-log-while-using-multiprocessing-in-python/
# for multiprocessing logging and/or buffered logging for I/O performance.

# ============================================================================
# Define custom level
CHATTY_LEVEL_NUM = 5
logging.addLevelName(CHATTY_LEVEL_NUM, "CHATTY")

def chatty(self, message, *args, **kws):
    # Yes, logger takes its '*args' as 'args'.
    if self.isEnabledFor(CHATTY_LEVEL_NUM):
        self._log(CHATTY_LEVEL_NUM, message, args, stacklevel=2, **kws)
logging.Logger.chatty = chatty

# ============================================================================
# Prettier logging for console output
class CustomFormatter(logging.Formatter):
    grey     = "\x1b[38;20m"
    yellow   = "\x1b[33;20m"
    green    = "\x1b[32;20m"
    blue     = "\x1b[36;20m"
    red      = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset    = "\x1b[0m"
    format   = "%(asctime)s [%(levelname)s] - %(message)s"

    FORMATS = {
        CHATTY_LEVEL_NUM: yellow   + format + " (%(filename)s:%(lineno)d) " + reset, # Added CHATTY level
        logging.DEBUG:    grey     + format + " (%(filename)s:%(lineno)d) " + reset,
        logging.INFO:     green    + format + reset,
        logging.WARNING:  blue     + format + " (%(filename)s:%(lineno)d) " + reset,
        logging.ERROR:    red      + format + " (%(filename)s:%(lineno)d) " + reset,
        logging.CRITICAL: bold_red + format + " (%(filename)s:%(lineno)d) " + reset
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno, self.format)
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)

# ============================================================================
slogger = logging.getLogger( 'sphenixprod' )
# Prevent duplicate handlers if this module is reloaded
if not slogger.hasHandlers():
    ch = logging.StreamHandler()
    ch.setFormatter(CustomFormatter())
    slogger.addHandler(ch)

CHATTY   = slogger.chatty # Added convenience function for lots of output
DEBUG    = slogger.debug
INFO     = slogger.info
WARN     = slogger.warning
ERROR    = slogger.error
CRITICAL = slogger.critical

