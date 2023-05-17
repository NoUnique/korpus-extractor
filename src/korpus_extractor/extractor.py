from types import SimpleNamespace
from typing import Any, Dict, List, Optional

import os
import re
import zipfile
from abc import ABC, abstractmethod

import msgspec


class Extractor(ABC):
    @abstractmethod
    def extract(self, corpus_path: str, output_path: str, **kwargs):
        raise NotImplementedError


class ZippedJsonExtractor(Extractor):
    def __init__(self):
        self.msgspec_classes = {}
        self.data_structure = []

    def _get_corpus_info_by_path(self, corpus_path: str) -> dict:
        raise NotImplementedError

    @staticmethod
    def read_filenames_in_zip(filepath, extension="json"):
        assert os.path.exists(filepath)
        fileinfo = []
        try:
            with open(filepath, "rb") as f:
                zipped_file = zipfile.ZipFile(f)
                fileinfo = [f.filename for f in zipped_file.infolist()]
                fileinfo = [f for f in fileinfo if f.endswith(f".{extension}")]
        except Exception as e:
            print(e)
        return sorted(fileinfo)

    def create_msgspec_classes(self, jq_expression: str):
        props = jq_expression.strip().split(".")[1:]
        pairs = list(zip(props, props[1:]))
        if len(props) == 1:
            pairs.insert(0, ("root", props[0]))
        else:
            pairs.insert(0, ("root", pairs[0][0]))
        print(pairs)

        # remove element for flatten dictionaries(|.[])
        i = 0
        while i < len(pairs):
            if not pairs[i][0].endswith("[]|") and pairs[i][0].endswith("|") and pairs[i][1] == "[]":
                del pairs[i]
                continue
            i += 1

        def _fn_type_identity(x):
            return x

        def _fn_type_list(x):
            return List[x]

        def _fn_type_dict(x):
            return Dict[str, x]

        self.data_structure = []
        for depth, (cls, field) in enumerate(pairs, start=0):
            class_name = re.sub(r"[\[\]\|]", "", cls).capitalize()
            field_name = field
            field_type = _fn_type_identity
            if field.endswith("[]|"):
                field_name = field[:-3]
                field_type = _fn_type_list
            elif field.endswith("|"):
                field_name = field[:-1]
                field_type = _fn_type_dict
            elif field.endswith("[]"):
                field_name = field[:-2]
                field_type = _fn_type_list
            self.data_structure.append(
                SimpleNamespace(
                    depth=depth,
                    class_name=class_name,
                    field_name=field_name,
                    field_type=field_type,
                )
            )

        for i in self.data_structure:
            print(i)

        self.msgspec_classes = {}
        for item in reversed(self.data_structure):
            class_name = "Item" if not item.class_name else item.class_name
            field_name = "item" if not item.field_name else item.field_name
            field_cls = self.msgspec_classes.get(item.depth + 1, Optional[str])
            field_cls = item.field_type(field_cls)
            print(class_name)
            print(field_name)
            print(field_cls)
            data_cls = msgspec.defstruct(class_name, [(field_name, field_cls, None)])
            self.msgspec_classes[item.depth] = data_cls
            if not item.field_name and item.field_type(str) == List[str]:
                field_type = self.msgspec_classes.get(item.depth + 1, Optional[str])
                self.msgspec_classes[item.depth] = List[field_type]

        for i in range(len(self.msgspec_classes)):
            print(self.msgspec_classes[i])

    def flatten_data(self, data: Any) -> List[Any]:
        def _flatten_data(data, data_structure, depth):
            if depth >= len(data_structure):
                return [data]  # If we exceed the depth, wrap 'data' as a list
            data_info = data_structure[depth]
            field_name = data_info.field_name
            if data_info.field_type.__name__.endswith("list"):
                flattened_list = []
                iterable = getattr(data, field_name)
                if not field_name:
                    iterable = data
                for item in iterable:
                    flattened_list.extend(_flatten_data(item, data_structure, depth + 1))
                return flattened_list
            elif data_info.field_type.__name__.endswith("dict"):
                flattened_dict = []
                for item in getattr(data, field_name).values():
                    flattened_dict.extend(_flatten_data(item, data_structure, depth + 1))
                return flattened_dict
            elif data_info.field_type.__name__.endswith("identity"):
                return _flatten_data(getattr(data, field_name), data_structure, depth + 1)
            return [data]

        return _flatten_data(data, self.data_structure, 0)
