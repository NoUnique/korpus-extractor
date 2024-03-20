import json
import textwrap
from types import SimpleNamespace
from typing import Any, Dict, List, Literal, Optional

import os
import re
import zipfile
from abc import ABC, abstractmethod

import msgspec


class Extractor(ABC):
    def __init__(self):
        self.function_map = {
            'sentence': self.extract_sentences,
            'document': self.extract_documents,
        }
        self.direction_map = {
            'sentence': 'sentence_extraction',
            'document': 'document_extraction'
        }

    def create_msgspec_classes_from_dict(self, structure_dict: dict):
        def _create_msgspec_class_from_dict(dict_obj, class_name="Root"):
            fields = []
            for key, value in dict_obj.items():
                if isinstance(value, dict):
                    # handle recursively nested dictionary
                    field_class = _create_msgspec_class_from_dict(value, class_name=key.capitalize())
                    fields.append((key, field_class, None))
                elif isinstance(value, list) and value:
                    # estimate the type of list items
                    elem = value[0]
                    if isinstance(elem, dict):
                        field_class = _create_msgspec_class_from_dict(elem, class_name=key.capitalize())
                        fields.append((key, List[field_class], None))
                    else:
                        fields.append((key, List[type(elem)], None))
                else:
                    fields.append((key, type(value), None))
            return msgspec.defstruct(class_name, fields)
        return _create_msgspec_class_from_dict(structure_dict)

    def extract_sentences(self, data: Any, directions: List[str]) -> List[Any]:
        def _create_flatten_list_from_jq(jq_expression: str):
            props = jq_expression.strip().split(".")[1:]
            pairs = list(zip(props, props[1:]))
            if len(props) == 1:
                pairs.insert(0, ("root", props[0]))
            else:
                pairs.insert(0, ("root", pairs[0][0]))

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

            flatten_list = []
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
                flatten_list.append(
                    SimpleNamespace(
                        depth=depth,
                        class_name=class_name,
                        field_name=field_name,
                        field_type=field_type,
                    )
                )
            return flatten_list

        def _extract_sentences(data, flatten_list, depth):
            if depth >= len(flatten_list):
                return [data]  # If we exceed the depth, wrap 'data' as a list
            data_info = flatten_list[depth]
            field_name = data_info.field_name
            if data_info.field_type.__name__.endswith("list"):
                flattened_list = []
                iterable = getattr(data, field_name)
                if not field_name:
                    iterable = data
                for item in iterable:
                    flattened_list.extend(_extract_sentences(item, flatten_list, depth + 1))
                return flattened_list
            elif data_info.field_type.__name__.endswith("dict"):
                flattened_dict = []
                for item in getattr(data, field_name).values():
                    flattened_dict.extend(_extract_sentences(item, flatten_list, depth + 1))
                return flattened_dict
            elif data_info.field_type.__name__.endswith("identity"):
                return _extract_sentences(getattr(data, field_name), flatten_list, depth + 1)
            return [data]

        transformed_sentences = []
        for direction in directions:
            flatten_list = _create_flatten_list_from_jq(direction)
            transformed_sentences.extend(_extract_sentences(data, flatten_list, 0))
        return transformed_sentences

    def extract_documents(self, root: Any, direction: str) -> List[Any]:
        transformed_documents = []
        exec(textwrap.dedent(direction).strip(), globals(), locals())
        return [json.dumps(document, ensure_ascii=False) for document in transformed_documents]

    @abstractmethod
    def extract(self, corpus_path: str, output_path: str, extraction_type: Literal['sentence, document']='sentence', **kwargs):
        raise NotImplementedError


class ZippedJsonExtractor(Extractor):
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

