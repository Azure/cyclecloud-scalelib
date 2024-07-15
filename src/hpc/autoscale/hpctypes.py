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

from typing_extensions import Literal

from hpc.autoscale.codeanalysis import hpcwrapclass

BucketId = typing.NewType("BucketId", str)
ClusterName = typing.NewType("ClusterName", str)
Hostname = typing.NewType("Hostname", str)
InstanceId = typing.NewType("InstanceId", str)
IpAddress = typing.NewType("IpAddress", str)
JobId = typing.NewType("JobId", str)
Location = typing.NewType("Location", str)
# Memory = typing.NewType("Size", float)
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

ResourceTypeAtom = typing.Union[str, int, float, bool, "Size"]
ResourceDict = typing.NewType("ResourceDict", typing.Dict[str, ResourceTypeAtom])
ResourceType = typing.Union[ResourceTypeAtom, typing.List[ResourceTypeAtom]]

SizeMagnitude = Literal["b", "k", "m", "g", "t", "p"]
MemoryMagnitude = SizeMagnitude


_MAG_CONVERSIONS = {
    "b": 1024**0,
    "k": 1024**1,
    "K": 1000**1,
    "m": 1024**2,
    "M": 1000**2,
    "g": 1024**3,
    "G": 1000**3,
    "t": 1024**4,
    "T": 1000**4,
    "p": 1024**5,
    "P": 1000**5,
}


def add_magnitude_conversion(suffix: str, magnitude: int) -> None:
    global _MAG_CONVERSIONS
    _MAG_CONVERSIONS[suffix] = magnitude


SizeValue = typing.Union[float, int]
# MemoryValue = SizeValue


@hpcwrapclass
class Size:
    def __init__(self, value: SizeValue, magnitude: SizeMagnitude) -> None:
        self.__value = value
        assert isinstance(value, (int, float)), type(value)
        self.__magnitude = magnitude
        assert (
            magnitude in _MAG_CONVERSIONS
        ), "Invalid magnitude {}, expected {}".format(
            magnitude, _MAG_CONVERSIONS.keys()
        )

    @property
    def value(self) -> SizeValue:
        return self.__value

    @property
    def magnitude(self) -> SizeMagnitude:
        return self.__magnitude

    def convert_to(self, new_mag: SizeMagnitude) -> "Size":
        if new_mag == self.magnitude:
            return Size(self.value, self.magnitude)
        new_value = (
            self.value * _MAG_CONVERSIONS[self.magnitude] / _MAG_CONVERSIONS[new_mag]
        )
        return Size(new_value, new_mag)

    def __float__(self) -> float:
        return float(self.value) * _MAG_CONVERSIONS[self.magnitude]

    @classmethod
    def value_of(cls, value: typing.Union[SizeValue, "Size", str]) -> "Size":
        return Size._value_of(Size, value)

    @classmethod
    def _value_of(
        cls, value_cls: type, value: typing.Union[SizeValue, "Size", str]
    ) -> "Size":
        if isinstance(value, Size):
            return value

        if isinstance(value, (float, int)):
            return value_cls(value, "b")

        if isinstance(value, str):
            if ":" in value:
                # strip out size::/memory:: etc
                value = value[value.rindex(":") + 1 :]

            if not value:
                raise RuntimeError(
                    "blank string can not be converted to %s" % value_cls.__name__
                )

            if not value[-1].isalpha():
                return value_cls(float(value), "b")

            mag_length = 1
            while value[-mag_length - 1].isalpha():
                mag_length += 1

            mag: SizeMagnitude = value[-mag_length:]  # type: ignore
            if mag not in _MAG_CONVERSIONS:
                raise RuntimeError(
                    (
                        "Unknown SizeMagnitude '{}'. To register custom magnitudes, call"
                        + " hpc.autoscale.hpctypes.add_magnitude_conversion('{}', N),"
                        + " where N is the number of bytes."
                    ).format(mag, mag)
                )
            num = float(value[:-mag_length])
            return value_cls(num, mag)
        raise RuntimeError("Unsupported memory value '{}'".format(value))

    def __int__(self) -> int:
        return int(self.value)

    def __add__(self, other: typing.Union[SizeValue, "Size"]) -> "Size":
        other = Size.value_of(other)
        b = float(self) + float(other)
        new_value = b / _MAG_CONVERSIONS[self.magnitude]
        return Size(new_value, self.magnitude)

    def __sub__(self, other: typing.Union[SizeValue, "Size"]) -> "Size":
        other = Size.value_of(other)
        b = float(self) - float(other)
        new_value = b / _MAG_CONVERSIONS[self.magnitude]
        return Size(new_value, self.magnitude)

    def __truediv__(self, other: typing.Union["Size", SizeValue]) -> "Size":
        if isinstance(other, Size):
            return Size(
                self.value / other.convert_to(self.magnitude).value, self.magnitude
            )
        return Size((self.value / other), self.magnitude)

    def __floordiv__(self, other: typing.Tuple[float, "Size"]) -> "Size":
        as_float: float
        if isinstance(other, Size):
            as_float = other.convert_to(self.magnitude).value
        elif isinstance(other, (float, int)):
            as_float = other
        return Size((self.value // as_float), self.magnitude)

    def __mul__(self, other: SizeValue) -> "Size":
        b = float(self) * float(other)
        new_value = b / _MAG_CONVERSIONS[self.magnitude]
        return Size(new_value, self.magnitude)

    def __float_eq(self, f1: float, f2: float) -> bool:
        # FYI we are comparing bytes here, so it will likely be tiny
        return abs(f1 - f2) < 10**-10

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, (int, float, Size)):
            return False
        other = Size.value_of(other)
        return self.__float_eq(float(self), float(other))

    def __gt__(self, other: typing.Union[SizeValue, "Size"]) -> bool:
        return float(self) > float(other)

    def __ge__(self, other: typing.Union[SizeValue, "Size"]) -> bool:
        me = float(self)
        them = float(other)
        return me > them or self.__float_eq(me, them)

    def __lt__(self, other: typing.Union[SizeValue, "Size"]) -> bool:
        return float(self) < float(other)

    def __le__(self, other: typing.Union[SizeValue, "Size"]) -> bool:
        me = float(self)
        them = float(other)
        return me < them or self.__float_eq(me, them)

    def __str__(self) -> str:
        return "{:.2f}{}".format(self.value, self.magnitude)

    def __repr__(self) -> str:
        return "{}{}".format(self.value, self.magnitude)

    def to_json(self) -> str:
        return "size::" + str(self)


class Memory(Size):
    def to_json(self) -> str:
        return "memory::" + str(self)

    @classmethod
    def value_of(cls, value: typing.Union[SizeValue, "Size", str]) -> "Memory":
        return Size._value_of(Memory, value)  # type: ignore

    def convert_to(self, new_mag: SizeMagnitude) -> "Size":
        if new_mag == self.magnitude:
            return Memory(self.value, self.magnitude)
        new_value = (
            self.value * _MAG_CONVERSIONS[self.magnitude] / _MAG_CONVERSIONS[new_mag]
        )
        return Memory(new_value, new_mag)
