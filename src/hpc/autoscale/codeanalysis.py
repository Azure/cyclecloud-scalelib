import inspect
import os
from typing import Any, Callable, List, Optional

from typeguard import typechecked

import hpc.autoscale.hpclogging as logging
from hpc.autoscale.hpclogging import apitrace

RUNTIME_TYPE_CHECKING = os.getenv("HPC_RUNTIME_CHECKS", "false").lower() == "true"
TRACE_FUNCTIONS: List[str] = ["all"]
WHITELIST_FUNCTIONS_TYPES = ["register_result_handler"]


def hpcwrap(function: Callable) -> Callable:

    if not RUNTIME_TYPE_CHECKING:
        return function

    if "all" in TRACE_FUNCTIONS or function.__name__ in TRACE_FUNCTIONS:

        def apitraceall(func: Callable) -> Callable:
            return apitrace(func, repro_level=True, fine_level=False, trace_level=True)

    else:

        def apitraceall(func: Callable) -> Callable:
            return func

    if function.__name__ in WHITELIST_FUNCTIONS_TYPES:
        # disable type checking, often because it uses subscripted types, sadly.
        typechecked_func = function
    else:
        typechecked_func = typecheck_function(function, apitraceall)

    def hpcwrapper(*args: Any, **kwargs: Any) -> Optional[Any]:
        if function.__name__ in WHITELIST_FUNCTIONS_TYPES:
            if not hasattr(hpcwrapper, "hpcwarned"):
                setattr(hpcwrapper, "hpcwarned", True)
                logging.warning(
                    "Runtime type checking is disabled for %s", function.__name__
                )
        return typechecked_func(*args, **kwargs)

    return hpcwrapper


def typecheck_function(
    function: Callable, apitrace: Callable[[Callable], Callable]
) -> Callable:
    def typecheck_wrap(*args: Any, **kwargs: Any) -> Any:

        from hpc.autoscale.node.node import Node  # noqa
        from hpc.autoscale.node.bucket import NodeBucket  # noqa

        from hpc.autoscale.job.job import Job  # noqa

        return typechecked(function)(*args, **kwargs)

    return typecheck_wrap


def hpcwrapclass(cls: type) -> type:
    method: Callable

    for method_name, method in inspect.getmembers(cls):  # type: ignore

        if not hasattr(method, "__call__"):
            continue

        if method_name != "__init__" and method_name.startswith("__"):
            continue

        # C functions are missing this and can't have runtime checks
        if not hasattr(method, "__module__"):
            continue
        sig = inspect.signature(method)
        if len(sig.parameters) == 0 or (
            len(sig.parameters) == 1 and "self" in sig.parameters
        ):
            continue

        setattr(cls, method_name, hpcwrap(method))
    return cls
