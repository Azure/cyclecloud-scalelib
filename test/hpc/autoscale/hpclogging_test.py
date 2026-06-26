from hpc.autoscale import hpclogging
import io
import logging.handlers
import os
from typing import Any, Tuple
import tempfile


class MockFileOwnerHandler:
    def __init__(self, path: str, uid: int, gid: int):
        self.path = path
        self.uid = uid
        self.gid = gid

    def __call__(self, path: str) -> Tuple[int, int]:
        assert path == self.path
        return self.uid, self.gid

def test_disable_log_rotation():
    fd, path = tempfile.mkstemp()
    # wrong uid/gid
    try:
        mock_fh = MockFileOwnerHandler(path, os.getuid() + 1, os.getgid() + 1)
        logger = hpclogging.HPCLogger("test", file_owner_handler=mock_fh)
        handler = logging.handlers.RotatingFileHandler(path, maxBytes=1000)
        # we will see a mismatch, so we will set maxBytes to 2^32 and log a warning
        logger.addHandler(handler)
        assert handler.maxBytes == 2**32
        assert logger.non_rotated_files == [path]
        logger.info("test")
        assert not logger.non_rotated_files
        handler.close()
        assert f"The following logs will not be rotated because the current user does not match the owner: {path}\ntest" == open(path).read().strip()
    finally:
        os.close(fd)
        os.remove(path)

    # correct uid/gid
    with open(path, "w"):
        pass
    try:
        mock_fh = MockFileOwnerHandler(path, os.getuid(), os.getgid())
        logger = hpclogging.HPCLogger("test", file_owner_handler=mock_fh)
        handler = logging.handlers.RotatingFileHandler(path, maxBytes=1000)
        # happy path - we do not touch maxBytes and there are no warnings
        logger.addHandler(handler)
        assert handler.maxBytes == 1000
        assert logger.non_rotated_files == []
        logger.info("test")
        assert not logger.non_rotated_files
        handler.close()
        assert f"test" == open(path).read().strip()
    finally:
        os.remove(path)


def test_root_chown() -> None:
    class MockFileHandler:
        fd, baseFilename = tempfile.mkstemp()
        mode = "a"
        encoding = None
        def _open(self) -> io.TextIOWrapper:
            raise RuntimeError()
        
        def __enter__(self) -> "MockFileHandler":
            return self
        
        def __exit__(self, *args: Any, **kwargs: Any) -> None:
            os.close(self.fd)
            os.remove(self.baseFilename)

    mock_fh = MockFileHandler()
    try:
        mock_fh._open()
        assert False
    except RuntimeError:
        pass

    orig_uid, orig_gid = hpclogging._UID, hpclogging._GID
    hpclogging._UID = hpclogging._GID = 0
    try:
        import pwd, grp
        user = pwd.getpwuid(os.getuid()).pw_name
        group = grp.getgrgid(os.getgid()).gr_name
        hpclogging.set_default_log_user(user, group)
        hpclogging.make_chown_handler(mock_fh)
        # assert mock_fh is patched with a new _open implementation
        mock_fh._open().close()

    finally:
        hpclogging._UID = orig_uid
        hpclogging._GID = orig_gid