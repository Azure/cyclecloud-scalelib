import grp
import inspect
import io
import logging
import logging.handlers
import logging.config
import os
import pwd
import sys
import time
import types
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple


_PID = "%-5s" % os.getpid()
_UID = os.getuid()
_GID = os.getgid()
_DEFAULT_LOG_USER = os.getenv("SCALELIB_LOG_USER")
_DEFAULT_LOG_GROUP = os.getenv("SCALELIB_LOG_GROUP")


def set_default_log_user(user: str, group: str) -> None:
    global _DEFAULT_LOG_USER, _DEFAULT_LOG_GROUP
    _DEFAULT_LOG_USER = user
    _DEFAULT_LOG_GROUP = group


class FileOwnerHandler:
    def __call__(self, path: str) -> Tuple[int, int]:
        if not os.path.exists(path):
            return _UID, _GID
        st_f = os.stat(path)
        return st_f.st_uid, st_f.st_gid


def make_chown_handler(
    hdlr: logging.handlers.RotatingFileHandler,
    file_owner_handler: FileOwnerHandler = FileOwnerHandler(),
) -> Any:

    log_uid = pwd.getpwnam(_DEFAULT_LOG_USER).pw_uid if _DEFAULT_LOG_USER else _UID
    log_gid = grp.getgrnam(_DEFAULT_LOG_GROUP).gr_gid if _DEFAULT_LOG_GROUP else _GID

    def _open_with_chown(self):
        if _UID == 0 and (log_uid, log_gid) != (_UID, _GID):
            with open(self.baseFilename, self.mode, encoding=self.encoding) as fd:
                fd.write("")
            os.chown(self.baseFilename, log_uid, log_gid)

        return open(self.baseFilename, self.mode, encoding=self.encoding)

    # override the method for all newly created files
    hdlr._open = types.MethodType(_open_with_chown, hdlr)

    # chown the existing file, which will almost always be created eagerly
    if (
        _UID == 0
        and (log_uid, log_gid) != (_UID, _GID)
        and os.path.exists(hdlr.baseFilename)
    ):
        cur_uid, cur_gid = file_owner_handler(hdlr.baseFilename)
        if (cur_uid, cur_gid) != (log_uid, log_gid):
            os.chown(hdlr.baseFilename, log_uid, log_gid)

    return hdlr


class HPCLogger(logging.Logger):
    def __init__(
        self,
        name: str,
        level: int = logging.DEBUG,
        file_owner_handler: FileOwnerHandler = FileOwnerHandler(),
    ) -> None:
        logging.Logger.__init__(self, name, level)
        self.non_rotated_files = []
        self.file_owner_handler = file_owner_handler

    def addHandler(self, hdlr: logging.Handler) -> None:
        if isinstance(hdlr, logging.handlers.RotatingFileHandler):
            if _UID == 0:
                # patches hdlr's _open function in constructor
                hdlr = make_chown_handler(hdlr)
            elif (_UID, _GID) != self.file_owner_handler(hdlr.baseFilename):
                self.non_rotated_files.append(hdlr.baseFilename)
                hdlr.maxBytes = 2 ** 32

        return super().addHandler(hdlr)

    def _log(  # type: ignore
        self,
        level,
        msg,
        args,
        exc_info=None,
        extra=None,
        stack_info=False,
        # stacklevel only exists in python 3.8
        *stacklevel,
        **stacklevelkw
    ):
        if self.non_rotated_files:
            to_log = self.non_rotated_files + []
            self.non_rotated_files = []
            # this will not recurse because self.non_rotated_files is empty
            self.warning(f"The following logs will not be rotated because the current user does not match the owner: {','.join(to_log)}")

        if extra is None:
            extra = {}
        if "context" not in extra:
            extra["context"] = _CONTEXT

        if "pid" not in extra:
            extra["pid"] = _PID

        logging.Logger._log(
            self,
            level,
            msg,
            args,
            exc_info,
            extra,
            stack_info,
            *stacklevel,
            **stacklevelkw
        )

    def fine(self, msg: str, *args: Any) -> None:
        if self.getEffectiveLevel() <= FINE:
            self._log(FINE, msg, args)

    def trace(self, msg: str, *args: Any) -> None:
        if self.getEffectiveLevel() <= TRACE:
            self._log(TRACE, msg, args)


class HPCRootLogger(HPCLogger, logging.RootLogger):
    def __init__(self, level: int = logging.WARNING):
        HPCLogger.__init__(self, "root", level)


def reprolog(func: Callable, args: Dict[str, Any], retval: Any) -> Any:

    if _REPRO_LOGGER.getEffectiveLevel() > REPRO:
        return

    if hasattr(retval, "to_dict"):
        retval = retval.to_dict()

    import jsonpickle

    _REPRO_LOGGER.log(
        REPRO,
        jsonpickle.encode(
            {
                "timestamp": time.ctime(),
                "instance_id": __INSTANCE_ID,
                "function": func,
                "args": args,
                "retval": retval,
            }
        ),
    )


logging.setLoggerClass(HPCLogger)
logging.root = HPCRootLogger(logging.root.level)


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
_CONTEXT = "[init]"


logging._nameToLevel["FINE"] = FINE
logging._nameToLevel["TRACE"] = TRACE
logging._nameToLevel["REPRO"] = REPRO
logging._levelToName[FINE] = "FINE"
logging._levelToName[TRACE] = "TRACE"
logging._levelToName[REPRO] = "REPRO"

getLogger = logging.getLogger
basicConfig = logging.basicConfig

log = logging.log
debug = logging.debug

# we are the ones adding fine/trace when we override the logger class above
try:
    fine = logging.getLogger().fine  # type: ignore
    trace = logging.getLogger().trace  # type: ignore
except AttributeError:
    print("fine/trace logging are disabled", file=sys.stderr)
    fine = trace = logging.debug
info = logging.info
warning = logging.warning
warn = logging.warn
error = logging.error
exception = logging.exception
critical = logging.critical


__CALL_ID = 10000
__INSTANCE_ID = time.time()


def apitrace(
    function: Callable,
    repro_level: bool = True,
    fine_level: bool = True,
    trace_level: bool = True,
) -> Callable:

    if not (repro_level or fine_level or trace_level):
        return function

    if hasattr(function, "is_apitraced"):
        return function

    def apitrace_wrapper(*args: Any, **kwargs: Any) -> Any:
        global __CALL_ID
        call_id = "invoke-{}".format(__CALL_ID)
        __CALL_ID += 1
        instance_id = "inst-{}".format(__INSTANCE_ID)

        sig = inspect.signature(function)
        arg_strs: List[str] = []
        param_names = list(sig.parameters.keys())
        self_arg = None
        args_dict = {}

        for n in range(len(args)):
            arg_name = param_names[min(n, len(param_names) - 1)]
            arg_value = args[n]
            args_dict[arg_name] = arg_value

            if arg_name == "self":
                if function.__name__ != "__init__":
                    self_arg = arg_value
                else:
                    self_arg = "__init__"
            else:
                arg_strs.append("{}={}".format(arg_name, repr(arg_value)))

        args_dict.update(kwargs)

        for arg_name, arg_value in kwargs.items():
            arg_strs.append("{}={}".format(arg_name, repr(arg_value)))

        if trace_level:
            trace(
                "TRACE_ENTER: [%s] [%s] %s invoke %s(%s)",
                instance_id,
                call_id,
                self_arg or "function",
                function.__name__,
                ", ".join(arg_strs),
            )

        if fine_level:
            fine(
                "ENTER: [%s] [%s] %s(%s)",
                instance_id,
                call_id,
                function.__name__,
                ", ".join(arg_strs),
            )

        ret_val = function(*args, **kwargs)
        if trace_level:
            trace(
                "TRACE_EXIT: [%s] [%s] %s(...) -> %s",
                instance_id,
                call_id,
                function.__name__,
                repr(ret_val),
            )

        if fine_level:
            fine(
                "EXIT: [%s] [%s] %s(...) -> %s",
                instance_id,
                call_id,
                function.__name__,
                repr(ret_val),
            )

        if repro_level:
            reprolog(function, args_dict, ret_val)

        return ret_val

    setattr(apitrace_wrapper, "is_apitraced", True)
    return apitrace_wrapper


__INITIALIZED = False


def initialize_logging(config: Optional[Dict[str, Any]] = None) -> None:
    global __INITIALIZED

    if __INITIALIZED:
        return

    if config is None:
        config = {}

    logging_section = config.get("logging", {})
    logging_config_file = logging_section.get("config_file")

    def errprint(msg: str) -> None:
        print(msg, file=sys.stderr)

    if logging_config_file:

        try:
            with open(logging_config_file, "r"):
                pass
        except FileNotFoundError:
            errprint("Logging conf file {} does not exist!".format(logging_config_file))
        except Exception as e:
            errprint(
                "Failed to open logging conf file {}! {}".format(logging_config_file, e)
            )
        else:
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

    __INITIALIZED = True


def set_context(ctx: str) -> None:
    global _CONTEXT
    _CONTEXT = ctx


def cut(columns: Iterable[int]) -> None:
    file_handlers = [x for x in logging.root.handlers if hasattr(x, "baseFilename")]

    fact = logging.getLogRecordFactory()

    for handler in file_handlers:
        if not handler.formatter:
            continue

        record = fact(
            name="cut",
            level=logging.INFO,
            pathname=__file__,
            lineno=100,
            msg="example line",
            args=(),
            exc_info=None,
        )
        setattr(record, "context", "[example]")

        # print(record.args)
        # print(record.getMessage())
        # for x in dir(record):
        #     print(x, getattr(record, x))
        msg = handler.formatter.format(record)
        print(repr(msg))
