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
    show_datetime = True
    grey     = "\x1b[38;20m"
    yellow   = "\x1b[33;20m"
    green    = "\x1b[32;20m"
    blue     = "\x1b[36;20m"
    red      = "\x1b[31;20m"
    bold_red = "\x1b[31;1m"
    reset    = "\x1b[0m"
    datetime_format = "%(asctime)s [%(levelname)s] - %(message)s"
    plain_format = "[%(levelname)s] - %(message)s"

    def _base_format(self):
        return self.datetime_format if self.show_datetime else self.plain_format

    def format(self, record):
        base_format = self._base_format()
        formats = {
            CHATTY_LEVEL_NUM: self.yellow + base_format + " (%(filename)s:%(lineno)d) " + self.reset, # Added CHATTY level
            logging.DEBUG:    self.grey + base_format + " (%(filename)s:%(lineno)d) " + self.reset,
            logging.INFO:     self.green + base_format + self.reset,
            logging.WARNING:  self.blue + base_format + " (%(filename)s:%(lineno)d) " + self.reset,
            logging.ERROR:    self.red + base_format + " (%(filename)s:%(lineno)d) " + self.reset,
            logging.CRITICAL: self.bold_red + base_format + " (%(filename)s:%(lineno)d) " + self.reset,
        }
        formatter = logging.Formatter(formats.get(record.levelno, base_format))
        return formatter.format(record)


def set_log_timestamps_enabled(enabled: bool):
    CustomFormatter.show_datetime = enabled

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

