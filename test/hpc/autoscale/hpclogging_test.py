from hpc.autoscale import hpclogging
import logging.handlers
import os
from typing import Tuple
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
    path = tempfile.mktemp()
    # wrong uid/gid
    with open(path, "w"):
        pass
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
