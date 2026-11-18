#!/usr/bin/env python3
"""Generate offline compact-protocol cases from pinned Parquet 57.3.2 format.rs.

Usage: python3 scripts/ci/generate_parquet_wire_goldens.py PATH_TO_UPSTREAM_FORMAT_RS
The upstream crate is an input file only; it is never a Cargo dependency.
"""

import hashlib
import re
import struct
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
OUTPUT = ROOT / "crates/alopex-sql/src/executor/bulk/parquet/wire_schema/golden_tests.rs"
UPSTREAM_SHA256 = "14fa0958710481a9616e902a32991baa3e998f69a5b0d07f4002a286fa59273f"
TYPES = {"Bool": 1, "Byte": 3, "I08": 3, "I16": 4, "I32": 5, "I64": 6, "Double": 7,
         "String": 8, "List": 9, "Struct": 12}


def zigzag(value):
    return (value << 1) ^ (value >> 63)


def varint(value):
    result = bytearray()
    while value > 0x7f:
        result.append((value & 0x7f) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def unwrap(name, outer):
    prefix = outer + "<"
    return name[len(prefix):-1] if name.startswith(prefix) and name.endswith(">") else None


def split_type(name):
    name = name.strip()
    return unwrap(name, "Option") or name


def type_code(name):
    name = split_type(name)
    if name == "bool": return TYPES["Bool"]
    if name == "i8": return TYPES["Byte"]
    if name == "i16": return TYPES["I16"]
    if name == "i32": return TYPES["I32"]
    if name == "i64": return TYPES["I64"]
    if name == "OrderedFloat<f64>": return TYPES["Double"]
    if name in ("String", "Vec<u8>"): return TYPES["String"]
    if unwrap(name, "Vec") is not None: return TYPES["List"]
    if name in enum_values: return TYPES["I32"]
    return TYPES["Struct"]


def field_header(field_id, previous, code):
    delta = field_id - previous
    if 0 < delta < 15:
        return bytes([(delta << 4) | code])
    return bytes([code]) + varint(zigzag(field_id))


def encode_value(name, mode, selected=None):
    name = split_type(name)
    if name == "bool": return b"\x01"
    if name == "i8": return b"\x01"
    if name in ("i16", "i32", "i64"): return varint(zigzag(1))
    if name == "OrderedFloat<f64>": return struct.pack("<d", 1.0)
    if name == "String" or name == "Vec<u8>": return b"\x01x"
    element = unwrap(name, "Vec")
    if element is not None:
        return bytes([0x10 | type_code(element)]) + encode_value(element, mode)
    if name in enum_values:
        value = selected if selected is not None else enum_values[name][0][1]
        return varint(zigzag(value))
    if name in unions:
        variant = selected if selected is not None else unions[name][0]
        variant_name, subtype, field_id = variant
        return field_header(field_id, 0, TYPES["Struct"]) + encode_value(subtype, mode) + b"\x00"
    if name in structs:
        output = bytearray()
        previous = 0
        for field_name, field_type, wire_type, field_id in structs[name]:
            if mode == "required" and unwrap(field_type, "Option") is not None:
                continue
            code = TYPES[wire_type]
            if code == TYPES["Bool"]:
                # Compact protocol puts the bool value in the field header.
                output += field_header(field_id, previous, 1)
            else:
                output += field_header(field_id, previous, code)
                output += encode_value(field_type, mode)
            previous = field_id
        return bytes(output) + b"\x00"
    raise ValueError(f"unknown type {name}")


def parse(source):
    global structs, unions, enum_values
    structs, unions, enum_values = {}, {}, {}
    impls = {}
    for name, body in re.findall(r"^impl crate::thrift::TSerializable for (\w+) \{(.*?)^\}", source, re.M | re.S):
        impls[name] = re.findall(r'TFieldIdentifier::new\("[^"]+", TType::(\w+), (\d+)\)', body)
    for name in re.findall(r"^pub struct (\w+)\(pub i32\);", source, re.M):
        block = re.search(r"^impl " + name + r" \{(.*?)^\}", source, re.M | re.S).group(1)
        values = re.findall(r"pub const (\w+): " + name + r" = " + name + r"\((-?\d+)\);", block)
        enum_values[name] = [(constant, int(value)) for constant, value in values]
        assert values, name
    for name, body in re.findall(r"^pub struct (\w+) \{(.*?)^\}", source, re.M | re.S):
        fields = re.findall(r"^\s+pub (\w+): ([^\n]+),$", body, re.M)
        wires = impls[name]
        assert len(fields) == len(wires), name
        assert all(TYPES[wire] == type_code(typ) for (_, typ), (wire, _) in zip(fields, wires)), name
        structs[name] = [(field, typ, wire, int(identifier))
                         for (field, typ), (wire, identifier) in zip(fields, wires)]
    for name, body in re.findall(r"^pub enum (\w+) \{(.*?)^\}", source, re.M | re.S):
        variants = re.findall(r"^\s+(\w+)\((\w+)\),$", body, re.M)
        wires = impls[name]
        assert len(variants) == len(wires), name
        assert all(wire == "Struct" for wire, _ in wires), name
        unions[name] = [(variant, subtype, int(identifier))
                        for (variant, subtype), (_, identifier) in zip(variants, wires)]
    assert (len(structs), len(unions), len(enum_values), len(impls)) == (50, 8, 8, 66)


def main():
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    source = Path(sys.argv[1]).read_bytes()
    if hashlib.sha256(source).hexdigest() != UPSTREAM_SHA256:
        raise SystemExit("upstream format.rs SHA-256 mismatch")
    parse(source.decode())
    cases = []
    for name, fields in structs.items():
        cases.append((name, "required", encode_value(name, "required")))
        if any(unwrap(typ, "Option") is not None for _, typ, _, _ in fields):
            cases.append((name, "all_present", encode_value(name, "all")))
    for name, variants in unions.items():
        for variant in variants:
            cases.append((name, variant[0], encode_value(name, "all", variant)))
    for name, values in enum_values.items():
        for constant, value in values:
            cases.append((name, constant, encode_value(name, "all", value)))
    output = [
        "// Generated from Apache Arrow parquet 57.3.2 format.rs; source SHA-256",
        "// " + UPSTREAM_SHA256,
        "// Regenerate with scripts/ci/generate_parquet_wire_goldens.py.",
        "use super::{TSerializable, format};",
        "use std::{fmt::Debug, io::Cursor};",
        "use std::sync::atomic::{AtomicUsize, Ordering};",
        "use thrift::protocol::{TCompactInputProtocol, TCompactOutputProtocol};",
        "static CASES: AtomicUsize = AtomicUsize::new(0);",
        "",
        "fn check<T: TSerializable + Debug + PartialEq>(name: &str, bytes: &[u8]) {",
        "    let mut protocol = TCompactInputProtocol::new(Cursor::new(bytes));",
        "    let decoded = T::read_from_in_protocol(&mut protocol).unwrap_or_else(|error| panic!(\"{name}: {error}\"));",
        "    let mut rewritten = Vec::new();",
        "    decoded.write_to_out_protocol(&mut TCompactOutputProtocol::new(&mut rewritten)).unwrap();",
        "    assert_eq!(rewritten, bytes, \"{name}\");",
        "    CASES.fetch_add(1, Ordering::Relaxed);",
        "}", "", "#[test]", "fn every_generated_type_matches_upstream_compact_vectors() {",
    ]
    for name, case, data in cases:
        literal = ", ".join(f"0x{byte:02x}" for byte in data)
        output.append(f'    check::<format::{name}>("{name}.{case}", &[{literal}]);')
    output += [f"    assert_eq!(CASES.load(Ordering::Relaxed), {len(cases)});", "}", ""]
    OUTPUT.write_text("\n".join(output))
    subprocess.run(["rustfmt", "--edition", "2024", str(OUTPUT)], check=True)
    print(f"wrote {len(cases)} cases: {len(structs)} structs, {len(unions)} unions, {len(enum_values)} enum newtypes")


if __name__ == "__main__":
    main()
