# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: streamvis/data.proto
"""Generated protocol buffer code."""
from google.protobuf.internal import builder as _builder
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n\x14streamvis/data.proto\"/\n\x05\x46ield\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x18\n\x04type\x18\x02 \x01(\x0e\x32\n.FieldType\"\x1e\n\tFloatList\x12\x11\n\x05value\x18\x01 \x03(\x02\x42\x02\x10\x01\"\x1c\n\x07IntList\x12\x11\n\x05value\x18\x01 \x03(\x05\x42\x02\x10\x01\"H\n\x06Values\x12\x1c\n\x06\x66loats\x18\x02 \x01(\x0b\x32\n.FloatListH\x00\x12\x18\n\x04ints\x18\x03 \x01(\x0b\x32\x08.IntListH\x00\x42\x06\n\x04\x64\x61ta\"W\n\x05Group\x12\n\n\x02id\x18\x01 \x01(\r\x12\r\n\x05scope\x18\x02 \x01(\t\x12\x0c\n\x04name\x18\x03 \x01(\t\x12\r\n\x05index\x18\x04 \x01(\r\x12\x16\n\x06\x66ields\x18\x05 \x03(\x0b\x32\x06.Field\"B\n\x06Points\x12\x10\n\x08group_id\x18\x01 \x01(\r\x12\r\n\x05\x62\x61tch\x18\x02 \x01(\r\x12\x17\n\x06values\x18\x03 \x03(\x0b\x32\x07.Values*\x1f\n\tFieldType\x12\t\n\x05\x46LOAT\x10\x00\x12\x07\n\x03INT\x10\x01\x62\x06proto3')

_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, globals())
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'streamvis.data_pb2', globals())
if _descriptor._USE_C_DESCRIPTORS == False:

  DESCRIPTOR._options = None
  _FLOATLIST.fields_by_name['value']._options = None
  _FLOATLIST.fields_by_name['value']._serialized_options = b'\020\001'
  _INTLIST.fields_by_name['value']._options = None
  _INTLIST.fields_by_name['value']._serialized_options = b'\020\001'
  _FIELDTYPE._serialized_start=366
  _FIELDTYPE._serialized_end=397
  _FIELD._serialized_start=24
  _FIELD._serialized_end=71
  _FLOATLIST._serialized_start=73
  _FLOATLIST._serialized_end=103
  _INTLIST._serialized_start=105
  _INTLIST._serialized_end=133
  _VALUES._serialized_start=135
  _VALUES._serialized_end=207
  _GROUP._serialized_start=209
  _GROUP._serialized_end=296
  _POINTS._serialized_start=298
  _POINTS._serialized_end=364
# @@protoc_insertion_point(module_scope)
