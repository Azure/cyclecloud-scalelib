__version__ = "0.1.3"
__release_version__ = __version__.replace("-SNAPSHOT", "")


def _append_build_number() -> None:
    import os
    import sys

    build_number = None

    try:
        if hasattr(sys, "_MEIPASS"):
            build_number_file = os.path.join(getattr(sys, "_MEIPASS"), "BUILD_NUMBER")
        else:
            build_number_file = os.path.join(os.path.dirname(__file__), "BUILD_NUMBER")

        if os.path.exists(build_number_file):
            with open(build_number_file, "r") as f:
                build_number = f.read().strip()
    except Exception:
        pass

    if build_number:
        global __version__
        __version__ = __version__.replace("-SNAPSHOT", "-%s" % build_number)


_append_build_number()
