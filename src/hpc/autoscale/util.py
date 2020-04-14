import inspect
import logging
import typing
import uuid as uuidlib


class IncrementingUUID:
    """
    A pseudo uuid utility to make debugging tests easier.
    """

    def __init__(self) -> None:
        self.current: typing.Dict[str, int] = {}

    def __call__(self, prefix: str) -> str:
        if prefix and not prefix.endswith("-"):
            prefix = prefix + "-"

        if prefix not in self.current:
            self.current[prefix] = 0

        ret = str(self.current[prefix])
        self.current[prefix] += 1
        return prefix + ret


def _uuid_func_impl(ignore: str) -> uuidlib.UUID:
    return uuidlib.uuid4()


_uuid_func = _uuid_func_impl


def set_uuid_func(uuid_func: typing.Any) -> None:
    global _uuid_func
    _uuid_func = uuid_func


def uuid(prefix: str = "") -> str:
    return str(_uuid_func(prefix))


T = typing.TypeVar("T")
K = typing.TypeVar("K")


def partition(
    node_list: typing.List[T], func: typing.Callable[[T], K]
) -> typing.Dict[K, typing.List[T]]:
    by_key: typing.Dict[K, typing.List[T]] = {}
    for node in node_list:
        key = func(node)
        if key not in by_key:
            by_key[key] = []
        by_key[key].append(node)
    return by_key


def tracelog(msg: str, *args: typing.Any) -> None:
    logging.log(logging.DEBUG // 2, msg, *args)


__CALL_ID = 10000


def apitrace(function: typing.Callable) -> typing.Callable:
    def wrapped(*args: typing.Any, **kwargs: typing.Any) -> typing.Any:
        global __CALL_ID
        call_id = "invoke-{}".format(__CALL_ID)
        __CALL_ID += 1
        # assert __CALL_ID < 10002

        sig = inspect.signature(function)
        arg_strs: typing.List[str] = []
        param_names = list(sig.parameters.keys())
        for n in range(len(args)):
            arg_name = param_names[n]
            arg_value = args[n]
            arg_strs.append("{}={}".format(arg_name, repr(arg_value)))

        for arg_name, arg_value in kwargs.items():
            arg_strs.append("{}={}".format(arg_name, repr(arg_value)))

        tracelog(
            "TRACE_ENTER: [%s] %s(%s)", call_id, function.__name__, ", ".join(arg_strs)
        )
        logging.debug(
            "ENTER: [%s] %s(%s)", call_id, function.__name__, ", ".join(arg_strs[1:])
        )
        ret_val = function(*args, **kwargs)
        tracelog(
            "TRACE_EXIT: [%s] %s -> %s",
            call_id,
            function.__name__,
            ", ".join(arg_strs),
            repr(ret_val),
        )
        logging.debug(
            "EXIT: [%s] %s(...) -> %s", call_id, function.__name__, repr(ret_val)
        )
        return ret_val

    return wrapped
