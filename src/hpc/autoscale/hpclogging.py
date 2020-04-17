import inspect
import logging
import logging.config
import os
import sys
import time
from typing import Any, Callable, Dict, List, Optional

import jsonpickle

CRITICAL = logging.CRITICAL
FATAL = logging.FATAL
ERROR = logging.ERROR
WARNING = logging.WARNING
WARN = logging.WARN
INFO = logging.INFO
DEBUG = logging.DEBUG
FINE = logging.DEBUG // 2
TRACE = 2
REPRO = 1
NOTSET = 0
_REPRO_LOGGER = logging.getLogger("repro")


logging._nameToLevel["FINE"] = FINE
logging._nameToLevel["TRACE"] = TRACE
logging._nameToLevel["REPRO"] = REPRO
logging._levelToName[FINE] = "FINE"
logging._levelToName[TRACE] = "TRACE"
logging._levelToName[REPRO] = "REPRO"

basicConfig = logging.basicConfig
getLogger = logging.getLogger
debug = logging.debug
info = logging.info
warning = logging.warning
warn = logging.warn
error = logging.error
exception = logging.exception
critical = logging.critical


def fine(msg: str, *args: Any) -> None:
    logging.log(FINE, msg, *args)


def trace(msg: str, *args: Any) -> None:
    logging.log(TRACE, msg, *args)


def reprolog(func: Callable, args: Dict[str, Any], retval: Any) -> Any:
    print("effective log level", _REPRO_LOGGER.getEffectiveLevel())
    if _REPRO_LOGGER.getEffectiveLevel() > REPRO:
        return

    if hasattr(retval, "to_dict"):
        retval = retval.to_dict()

    _REPRO_LOGGER.log(
        REPRO,
        jsonpickle.encode(
            {
                "timestamp": time.ctime(),
                "function": func,
                "args": args,
                "retval": retval,
            }
        ),
    )


__CALL_ID = 10000


def apitrace(
    function: Callable,
    repro_level: bool = True,
    debug_level: bool = True,
    trace_level: bool = True,
) -> Callable:

    if not (debug_level or trace_level):
        return function

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        global __CALL_ID
        call_id = "invoke-{}".format(__CALL_ID)
        __CALL_ID += 1

        sig = inspect.signature(function)
        arg_strs: List[str] = []
        param_names = list(sig.parameters.keys())
        self_arg = None
        args_dict = {}

        for n in range(len(args)):
            arg_name = param_names[n]
            arg_value = args[n]
            args_dict[arg_name] = arg_value

            if arg_name == "self":
                self_arg = arg_value
            else:
                arg_strs.append("{}={}".format(arg_name, repr(arg_value)))

        args_dict.update(kwargs)

        for arg_name, arg_value in kwargs.items():
            arg_strs.append("{}={}".format(arg_name, repr(arg_value)))

        if trace_level:
            trace(
                "TRACE_ENTER: [%s] %s invoke %s(%s)",
                call_id,
                self_arg or "function",
                function.__name__,
                ", ".join(arg_strs),
            )

        if debug_level:
            logging.debug(
                "ENTER: [%s] %s(%s)", call_id, function.__name__, ", ".join(arg_strs),
            )

        ret_val = function(*args, **kwargs)
        if trace_level:
            trace(
                "TRACE_EXIT: [%s] %s(...) -> %s",
                call_id,
                function.__name__,
                repr(ret_val),
            )

        if debug_level:
            logging.debug(
                "EXIT: [%s] %s(...) -> %s", call_id, function.__name__, repr(ret_val)
            )

        if debug_level:
            reprolog(function, args_dict, ret_val)

        return ret_val

    return wrapped


def initialize_logging(config: Optional[Dict[str, Any]] = None) -> None:
    if config is None:
        config = {}

    logging_section = config.get("logging", {})
    logging_config_file = logging_section.get("config_file")

    if logging_config_file:
        logging.config.fileConfig(logging_config_file)
    elif logging_section.get("config"):
        logging.config.dictConfig(logging_section.get("config"))
    else:
        config_path = os.getenv("AUTOSCALE_LOG_CONFIG", "../conf/logging.conf")
        if os.path.exists(config_path):
            logging.config.fileConfig(config_path)
        else:
            basicConfig(
                stream=sys.stderr,
                format="%(asctime)s %(levelname)s: %(message)s",
                level=DEBUG,
            )
