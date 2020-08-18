"""
    This module is to be used for two purposes.
    1) As a shorthand for importing types in python2/3 compliant comments.
        # type: hpctypes.Node
        These types are only imported if typing.TYPE_CHECKING is true, so this should be imported like
        
        import typing
        if typing.TYPE_CHECKING:
            from hpc.autoscale import hpctypes  # noqa
        
        This prevents overimporting of unused types etc.
    
    2) To define new types (used for type checking _only_) that are used throughout the code.
"""
# pylint: disable=invalid-name
import typing

from hpc.autoscale.codeanalysis import hpcwrapclass
from typing_extensions import Literal

BucketId = typing.NewType("BucketId", str)
ClusterName = typing.NewType("ClusterName", str)
Hostname = typing.NewType("Hostname", str)
IpAddress = typing.NewType("IpAddress", str)
JobId = typing.NewType("JobId", str)
Location = typing.NewType("Location", str)
# Memory = typing.NewType("Memory", float)
NodeArrayName = typing.NewType("NodeArrayName", str)
NodeId = typing.NewType("NodeId", str)
NodeName = typing.NewType("NodeName", str)
NodeStatus = typing.NewType("NodeStatus", str)
OperationId = typing.NewType("OperationId", str)
PlacementGroup = typing.NewType("PlacementGroup", str)
RequestId = typing.NewType("RequestId", str)
SubnetId = typing.NewType("SubnetId", str)
VMSize = typing.NewType("VMSize", str)
VMFamily = typing.NewType("VMFamily", str)

ResourceTypeAtom = typing.Union[str, int, float, bool]
ResourceDict = typing.NewType("ResourceDict", typing.Dict[str, ResourceTypeAtom])
ResourceType = typing.Union[ResourceTypeAtom, typing.List[ResourceTypeAtom]]

MemoryMagnitude = Literal["b", "k", "m", "g", "t", "p"]


_MAG_CONVERSIONS = {
    "b": 1024 ** 0,
    "k": 1024 ** 1,
    "K": 1000 ** 1,
    "m": 1024 ** 2,
    "M": 1000 ** 2,
    "g": 1024 ** 3,
    "G": 1000 ** 3,
    "t": 1024 ** 4,
    "T": 1000 ** 4,
    "p": 1024 ** 5,
    "P": 1000 ** 5,
}

MemoryValue = typing.Union[float, int]


@hpcwrapclass
class Memory:
    def __init__(self, value: MemoryValue, magnitude: MemoryMagnitude) -> None:
        self.__value = value
        assert isinstance(value, (int, float)), type(value)
        self.__magnitude = magnitude
        assert (
            magnitude in _MAG_CONVERSIONS
        ), "Invalid magnitude {}, expected {}".format(
            magnitude, _MAG_CONVERSIONS.keys()
        )

    @property
    def value(self) -> MemoryValue:
        return self.__value

    @property
    def magnitude(self) -> MemoryMagnitude:
        return self.__magnitude

    def convert_to(self, new_mag: MemoryMagnitude) -> "Memory":
        if new_mag == self.magnitude:
            return Memory(self.value, self.magnitude)
        new_value = (
            self.value * _MAG_CONVERSIONS[self.magnitude] / _MAG_CONVERSIONS[new_mag]
        )
        return Memory(new_value, new_mag)

    def __float__(self) -> float:
        return float(self.value) * _MAG_CONVERSIONS[self.magnitude]

    @classmethod
    def value_of(cls, value: typing.Union[MemoryValue, "Memory", str]) -> "Memory":
        if isinstance(value, Memory):
            return value

        if isinstance(value, (float, int)):
            return Memory(value, "b")

        if isinstance(value, str):
            if not value:
                raise RuntimeError("blank string can not be converted to Memory")

            if not value[-1].isalpha():
                return Memory(float(value), "b")

            mag: MemoryMagnitude = value[-1]  # type: ignore
            assert mag in _MAG_CONVERSIONS
            num = float(value[:-1])
            return Memory(num, mag)
        raise RuntimeError("Unsupported memory value '{}'".format(value))

    def __add__(self, other: typing.Union[MemoryValue, "Memory"]) -> "Memory":
        other = Memory.value_of(other)
        b = float(self) + float(other)
        new_value = b / _MAG_CONVERSIONS[self.magnitude]
        return Memory(new_value, self.magnitude)

    def __sub__(self, other: typing.Union[MemoryValue, "Memory"]) -> "Memory":
        other = Memory.value_of(other)
        b = float(self) - float(other)
        new_value = b / _MAG_CONVERSIONS[self.magnitude]
        return Memory(new_value, self.magnitude)

    def __truediv__(self, other: typing.Union["Memory", MemoryValue]) -> "Memory":
        if isinstance(other, Memory):
            return Memory(
                self.value / other.convert_to(self.magnitude).value, self.magnitude
            )
        return Memory((self.value / other), self.magnitude)

    def __floordiv__(self, other: float) -> "Memory":
        return Memory((self.value // other), self.magnitude)

    def __mul__(self, other: MemoryValue) -> "Memory":
        b = float(self) * float(other)
        new_value = b / _MAG_CONVERSIONS[self.magnitude]
        return Memory(new_value, self.magnitude)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (int, float, Memory)):
            return False

        other = Memory.value_of(other)
        # FYI we are comparing bytes here, so it will likely be tiny
        return abs(float(self) - float(other)) < 10 ** -10

    def __gt__(self, other: MemoryValue) -> bool:
        return float(self) > float(other)

    def __lt__(self, other: MemoryValue) -> bool:
        return float(self) > float(other)

    def __str__(self) -> str:
        return "{}{}".format(self.value, self.magnitude)

    def __repr__(self) -> str:
        return str(self)

    def to_json(self) -> str:
        return str(self)
