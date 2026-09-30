import protoparser
import os
from typing import Any, List, Optional, TextIO


INPUT_DIR = os.path.expanduser(
    os.path.join(
        os.getenv("CLUSTER_SERVICE_DIR", "DEFINE_CLUSTER_SERVICE_DIR"),
        "src",
        "clusters",
        "protocols",
    )
)
PY3 = os.getenv("PY3", "0") == "1"
OUTPUT_DIR = "generated"
output_dir = OUTPUT_DIR
APPEND = "cluster_service_client_impl.py" if PY3 else "cluster_service_server_impl.py"


def pytype(gtype: str) -> str:

    if gtype in [
        "StringValue",
        "string",
        "google.protobuf.StringValue",
        "BufferedAny",
        "microsoft.batchcommon.protobuf.BufferedAny",
        "microsoft.batchcommon.protobuf.BufferedByteString",
        "microsoft.batchcommon.LogSearchParametersMessage", # TODO
    ]:
        return "str"
    elif gtype in [
        "IntValue",
        "google.protobuf.IntValue",
        "int32",
        "Int32Value",
        "google.protobuf.Int32Value",
    ]:
        return "int"
    elif gtype in ["google.protobuf.FloatValue"]:
        return "float"
    elif gtype in ["google.protobuf.BoolValue", "BoolVaue"]:
        return "bool"
    elif gtype in [
        "google.protobuf.Timestamp",
        "Timestamp",
        "google.protobuf.Duration",
    ]:
        # TODO
        return "str"
    elif gtype in ["google.protobuf.Any", "Any"]:
        # TODO
        return "typing.Any"
    elif gtype in [
        "StringKeyValueCollection",
        "microsoft.batchcommon.protobuf.StringKeyValueCollection",
    ]:
        return "dict"
    elif gtype in [
        "microsoft.batchcommon.protobuf.StringKeyValuePair",
        "StringKeyValuePair",
    ]:
        return "typing.Tuple[str, str]"
    elif gtype.startswith("microsoft.batchclusters") or gtype[0].isupper():
        fmt = "'%s'" if PY3 else "%s"
        return fmt % gtype.replace("microsoft.batchclusters.", "")
    elif gtype.startswith("microsoft") or gtype[0].isupper():
        return "str"
    return gtype


class PrinterBlock:
    def __init__(self, printer: "Printer") -> None:
        self.printer = printer
        self.init_indent = -1

    def __enter__(self) -> "Printer":
        self.init_indent = self.printer._indent
        self.printer.indent()
        return self.printer

    def __exit__(self, *args: Any) -> bool:
        self.printer._indent = self.init_indent
        return True


class Printer:
    def __init__(self, writer: TextIO) -> None:
        self._indent = 0
        self.writer = writer

    def indent(self) -> None:
        self._indent += 4

    def outdent(self) -> None:
        self._indent -= 4
        assert self._indent >= 0

    def block(self) -> "PrinterBlock":
        return PrinterBlock(self)

    def __call__(self, msg: str) -> None:
        self.writer.write(" " * self._indent)
        self.writer.write(msg)
        self.writer.write("\n")

    def close(self) -> None:
        self.writer.close()


def _add_json(name: str, printer: "Printer"):
    printer("")
    if PY3:
        printer(f"def to_json(self) -> dict:")
    else:
        printer(f"def to_json(self):")
    printer(f"    ret = dict()")
    printer(f"    ret['_type'] = '{name}'")
    printer(f"    for attr in dir(self):")
    printer(f"        if not attr[0].islower():")
    printer(f"            continue")
    printer(f"        value = getattr(self, attr)")
    printer(f"        if hasattr(value, '__call__'):")
    printer(f"            continue")
    printer(f"        if hasattr(value, 'to_json'):")
    printer(f"            ret[attr] = value.to_json()")
    printer(f"        elif isinstance(value, list):")
    printer(f"            ret[attr] = []")
    printer(f"            for v in value:")
    printer(f"                if hasattr(v, 'to_json'):")
    printer(f"                    v = v.to_json()")
    printer(f"                ret[attr].append(v)")
    printer(f"        else:")
    printer(f"            ret[attr] = value")
    printer(f"    return ret")
    printer(f"")
    printer(f"@classmethod")
    if PY3:
        printer(f"def from_json(cls, d: dict) -> '{name}':")
    else:
        printer(f"def from_json(cls, d):")
    printer(f"    args = dict()")
    printer(f"    for key, value in d.items():")

    printer(f"        if key.startswith('_'):")
    printer(f"            continue")
    # printer(f"        print('HHH', key, type(value))")
    printer(f"        if isinstance(value, dict):")
    printer(f"            if '_type' in value:")
    printer(f"                clazz = globals()[value['_type']]")
    printer(f"                args[key] = clazz.from_json(value)")
    printer(f"            else:")
    printer(f"                args[key] = value")
    printer(f"        elif isinstance(value, list):")
    printer(f"            args[key] = []")
    printer(f"            for v in value:")
    # printer(f"                print('GGG', v['_type'])")
    printer(f"                if '_type' in v:")
    printer(f"                     clazz = globals()[v['_type']]")
    printer(f"                     v = clazz.from_json(v)")
    printer(f"                args[key].append(v)")
    # printer(f"                print('GGG', type(v))")
    printer(f"        else:")
    printer(f"            args[key] = value")
    # printer(f"    print('RET', cls, args)")
    printer(f"    return cls(**args)")


def _add_json_enum(name: str, printer: "Printer"):
    printer("")
    if PY3:
        printer(f"def to_json(self) -> dict:")
    else:
        printer(f"def to_json(self):")
    printer(f"    ret = dict()")
    printer(f"    ret['_type'] = '{name}'")
    printer(f"    ret['_value'] = self.value")
    printer(f"    return ret")
    printer(f"")

    printer(f"@classmethod")
    if PY3:
        printer(f"def from_json(cls, d: dict) -> '{name}':")
        printer(f"    return cls(d['_value'])")
    else:
        printer(f"def from_json(cls, d):")
        printer(f"    return d['_value']")


IMPORTS = set()


class Generator:
    def __init__(self, name: str, path: Optional[str] = None) -> None:
        self.name = name
        if path:
            self.path = path
            self.printer = Printer(open(self.path, "a"))
        else:
            self.path = os.path.join(
                *([OUTPUT_DIR] + list(p.package.split(".")) + [name + ".py"])
            )
            par_dir = os.path.dirname(self.path)
            if not os.path.exists(par_dir):
                os.makedirs(par_dir)
            self.printer = Printer(open(self.path, "w"))

        if PY3:
            if "typing" not in IMPORTS:
                self.printer("import typing")
                IMPORTS.add("typing")

            if "enum" not in IMPORTS:
                self.printer("import enum")
                IMPORTS.add("enum")

    def add_import(self, ref_type: str) -> None:
        if ref_type in IMPORTS:
            return
        if pytype(ref_type) != ref_type:
            pass
        elif "." in ref_type:
            self.printer(f"from {ref_type} import {ref_type.split('.')[-1]}")
        elif any([x.isupper() for x in ref_type]):
            pass
            # self.printer(f"from .{ref_type} import {ref_type}")
        IMPORTS.add(ref_type)

    def handle_imports(self, obj: Any) -> None:
        for imp in self._collect_imports(obj):
            self.add_import(imp)

    def handle(self, obj: Any) -> None:
        self._handle(obj)
        self.printer.close()

    def _handle(self, obj: Any) -> None:
        raise NotImplementedError("")

    def _collect_imports(self, obj: Any) -> List[str]:
        raise NotImplementedError("")


class ServiceGenerator(Generator):
    def _collect_imports(self, service: Any) -> List[str]:
        ret = []
        for f in service.functions:
            ret.append(f.in_type)
            ret.append(f.out_type)
        return ret

    def _handle(self, obj: Any) -> None:
        service = obj

        self.printer("")
        self.printer("")
        self.printer(f"class {self.name}:")
        with self.printer.block():
            self.printer("")

            for f in service.functions:
                if PY3:
                    self.printer(
                        f"def {f.name}(self, msg: {pytype(f.in_type)}) -> {pytype(f.out_type)}:"
                    )
                else:
                    self.printer(f"def {f.name}(self, msg):")

                    with self.printer.block():
                        self.printer('"""')
                        self.printer(f"    @msg: {pytype(f.in_type)}")
                        self.printer(f"    @returns: {pytype(f.out_type)}")
                        self.printer('"""')
                self.printer(f"    raise NotImplementedError('{f.name}')")
                self.printer("")
            _add_json(self.name, self.printer)


class MessageGenerator(Generator):
    def _collect_imports(self, message: Any) -> List[str]:
        return list(message.fields)

    def _handle(self, message: Any) -> None:
        self.printer("")
        self.printer("")
        self.printer(f"class {self.name}:")
        with self.printer.block():
            self.printer("")

            init_str = "def __init__(self"
            for f in message.fields:
                if f.type != "repeated":
                    if PY3:
                        init_str += f", {f.name}: {pytype(f.val_type)}"
                    else:
                        init_str += f", {f.name}"
                else:
                    if PY3:
                        init_str += f", {f.name}: typing.List[{pytype(f.val_type)}]"
                    else:
                        init_str += f", {f.name}"
            if PY3:
                init_str += ") -> None:"
            else:
                init_str += "):"

            self.printer(init_str)
            with self.printer.block():
                if not PY3:
                    self.printer('"""')
                    for f in message.fields:
                        self.printer(f"    @{f.name} : {pytype(f.val_type)}")
                    self.printer('"""')

                for f in message.fields:
                    self.printer(f"self.{f.name} = {f.name}")
                if not message.fields:
                    self.printer("pass")
            _add_json(self.name, self.printer)


class EnumGenerator(Generator):
    def _collect_imports(self, enum: Any) -> List[str]:
        return list(enum.fields)

    def _handle(self, enum: Any) -> None:

        self.printer("")
        if PY3:
            self.printer(f"class {self.name}(enum.Enum):")
        else:
            self.printer(f"class {self.name}:")
        with self.printer.block():
            index = 1
            for f in enum.fields:
                self.printer(f"{f.name} = {index}")
                index += 1
            _add_json_enum(self.name, self.printer)


def main() -> None:
    if PY3:
        single_path = os.path.join(OUTPUT_DIR, "cluster_service_client.py")
    else:
        single_path = os.path.join(OUTPUT_DIR, "cluster_service_plugin.py")
    if os.path.exists(single_path):
        os.remove(single_path)
    all_proto_paths = []
    for dirpath, dirnames, filenames in os.walk(INPUT_DIR):
        print(dirpath)
        if "./node" == dirpath:
            continue
        proto_paths = [
            os.path.join(dirpath, f) for f in filenames if f.endswith(".proto")
        ]
        all_proto_paths += proto_paths

    for n, proto_path in enumerate(all_proto_paths):
        print(f"{n + 1}/{len(all_proto_paths)} - {proto_path}")
        with open(proto_path) as fr:
            p = protoparser.parse(
                fr.read().replace("optional", "")
            )  # hack - this parser doesn't support optional fields
            for service_name, service in p.services.items():
                ServiceGenerator(service_name, single_path).handle(service)
            for message_name, message in p.messages.items():
                MessageGenerator(message_name, single_path).handle(message)
            for enum_name, enum in p.enums.items():
                EnumGenerator(enum_name, single_path).handle(enum)

    if APPEND:
        with open(APPEND) as fr:
            with open(single_path, "a") as fw:
                fw.write("\n")
                fw.write(fr.read())


if __name__ == "__main__":
    main()
