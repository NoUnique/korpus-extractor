from types import SimpleNamespace
from typing import Any, Dict, List, Literal, Optional

import json
import os
import re
import textwrap
import zipfile
from abc import ABC, abstractmethod

import msgspec
import yaml


class Extractor(ABC):
    def __init__(self):
        self.function_map = {
            "sentence": self.extract_sentences,
            "document": self.extract_documents,
        }
        self.direction_map = {
            "sentence": "sentence_extraction",
            "document": "document_extraction",
        }

    def _load_config(self, config_path):
        config = {}
        try:
            with open(config_path, encoding="utf-8") as f:
                config = yaml.safe_load(f)
            if "data_structure" in config:
                print(config["data_structure"])
                config["data_structure"] = json.loads(config["data_structure"])
        except FileNotFoundError:
            raise FileNotFoundError(f"File not found: {config_path}")
        except Exception as e:
            raise e
        return config

    def parse_size_limit(self, size_str: str) -> int:
        """Parse size string like '256m', '1g' to bytes"""
        if not size_str:
            return 256 * 1024 * 1024  # default 256MB
        
        size_str = size_str.lower().strip()
        match = re.match(r'^(\d+(?:\.\d+)?)\s*([kmgt]?)b?$', size_str)
        if not match:
            raise ValueError(f"Invalid size format: {size_str}")
        
        number, unit = match.groups()
        number = float(number)
        
        units = {'': 1, 'k': 1024, 'm': 1024**2, 'g': 1024**3, 't': 1024**4}
        return int(number * units.get(unit, 1))

    def get_split_file_path(self, output_path: str, index: int) -> str:
        """Generate split file path with index"""
        dir_path = os.path.dirname(output_path)
        base_name = os.path.basename(output_path)
        name_without_ext = os.path.splitext(base_name)[0]
        ext = os.path.splitext(base_name)[1]
        
        # Create subdirectory with base name
        split_dir = os.path.join(dir_path, name_without_ext)
        if not os.path.exists(split_dir):
            os.makedirs(split_dir, exist_ok=True)
        
        return os.path.join(split_dir, f"{name_without_ext}-{index:05d}{ext}")

    def create_msgspec_classes_from_dict(self, structure_dict: dict):
        def _create_msgspec_class_from_dict(dict_obj, class_name="Root"):
            fields = []
            for key, value in dict_obj.items():
                # Convert invalid field names that start with digits
                field_name = key
                if key and key[0].isdigit():
                    field_name = f"_{key}"
                
                if isinstance(value, dict):
                    # handle recursively nested dictionary
                    field_class = _create_msgspec_class_from_dict(value, class_name=key.capitalize())
                    fields.append((field_name, Optional[field_class], None))
                elif isinstance(value, list):
                    if value:
                        # estimate the type of list items
                        elem = value[0]
                        if isinstance(elem, dict):
                            field_class = _create_msgspec_class_from_dict(elem, class_name=key.capitalize())
                            fields.append((field_name, Optional[List[field_class]], None))
                        else:
                            if isinstance(elem, str):
                                fields.append((field_name, Optional[List[str]], None))
                            else:
                                fields.append((field_name, Optional[List[type(elem)]], None))
                    else:
                        fields.append((field_name, Optional[List], None))
                elif value is None:
                    fields.append((field_name, Optional[Any], None))
                else:
                    if isinstance(value, str):
                        fields.append((field_name, Optional[str], None))
                    else:
                        fields.append((field_name, Optional[type(value)], None))
            return msgspec.defstruct(class_name, fields)

        return _create_msgspec_class_from_dict(structure_dict)

    def extract_sentences(self, data: Any, directions: List[str], **kwargs) -> List[Any]:
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

    def extract_documents(self, root: Any, direction: str, **kwargs) -> List[Any]:
        transformed_documents = []
        exec(textwrap.dedent(direction).strip(), globals(), locals())
        return [json.dumps(document, ensure_ascii=False) for document in transformed_documents]

    @abstractmethod
    def extract(
        self,
        corpus_path: str,
        output_path: str,
        extraction_type: Literal["sentence", "document"] = "sentence",
        **kwargs,
    ):
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
