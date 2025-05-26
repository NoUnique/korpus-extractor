from typing import List, Literal, Optional

import collections
import concurrent.futures
import functools
import itertools
import os
import re
import threading
import zipfile

import msgspec
import psutil

from .extractor import ZippedJsonExtractor


class ModuExtractor(ZippedJsonExtractor):
    def __init__(self, config=None):
        if config:
            if os.path.exists(config):
                self.corpus_info = {config: self._load_config(config)}
            else:
                default_config_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), "configs", "nikl", f"{config}.yaml"
                )
                if os.path.exists(default_config_path):
                    self.corpus_info = {config: self._load_config(default_config_path)}
                else:
                    raise FileNotFoundError(f"Config file not found: {default_config_path}")
        else:
            self.corpus_info = {}
            config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs", "nikl")
            for yaml_file in os.listdir(config_dir):
                if yaml_file.endswith(".yaml"):
                    config_name = yaml_file[:-5]
                    config_path = os.path.join(config_dir, yaml_file)
                    try:
                        self.corpus_info[config_name] = self._load_config(config_path)
                    except Exception as e:
                        print(f"Error loading {config_path}: {e}")
        super().__init__()

    def _get_corpus_info_by_path(self, corpus_path: str) -> dict:
        for corpus_name, corpus_info in self.corpus_info.items():
            if corpus_info is None:
                continue
            filename = re.sub(
                f".{corpus_info['compressed_format']}$",
                "",
                os.path.basename(corpus_path),
            )
            if "file_names" in corpus_info:
                for file_name in corpus_info["file_names"]:
                    if filename == file_name:
                        return corpus_info
        raise ValueError(f"corpus_path '{corpus_path}' is not valid. No matching configuration found.")

    def extract(
        self,
        corpus_path: str,
        output_path: str,
        extraction_type: Literal["sentence", "document"] = "sentence",
        num_workers: Optional[int] = os.cpu_count(),
        max_memory_ratio: float = 0.5,
        **kwargs,
    ):
        corpus_info = self._get_corpus_info_by_path(corpus_path)
        msgspec_class = self.create_msgspec_classes_from_dict(corpus_info["data_structure"])

        _extract = self.function_map.get(extraction_type, self.extract_sentences)
        _direction = corpus_info[self.direction_map.get(extraction_type, "sentence")]
        if not _extract or not _direction:
            raise ValueError(f"Extraction type {extraction_type} is not valid.")

        def _read_msgspec_in_zipobj(zipobj, filename):
            data = []
            try:
                with zipobj.open(filename) as fj:
                    decoded_data = fj.read()
                    if corpus_info.get("file_encoding", "utf-8") != "utf-8":
                        decoded_data = decoded_data.decode(corpus_info["file_encoding"])
                    _data = msgspec.json.decode(decoded_data, type=msgspec_class)
                    _data = _extract(_data, _direction, compressed_filename=filename)
                    data = _data
            except msgspec.ValidationError as e:
                print(f"msgspec.ValidationError: {filename} in {zipobj.filename}")
                print(e)
            except msgspec.DecodeError as e:
                print(f"msgspec.DecodeError: {filename} in {zipobj.filename}")
                print(e)
            except Exception as e:
                print(e)
            finally:
                return data

        def _get_available_memory_ratio() -> float:
            return psutil.virtual_memory().available * 100 / psutil.virtual_memory().total

        def _progress_callback(
            lock: List[threading.Lock], tasks_completed: List[int], tasks_total: int, _: concurrent.futures.Future
        ) -> None:
            indexes = [int(tasks_total * (i / 10)) for i in range(1, 11)]
            with lock[0]:
                tasks_completed[0] += 1
                percent = tasks_completed[0] / tasks_total * 100
                line_end = "\n" if tasks_completed[0] in indexes else "\r"
                print(f"{tasks_completed[0]:#5d} / {tasks_total} ({percent:6.2f} %) completed", end=line_end)

        _filenames = self.read_filenames_in_zip(corpus_path, extension=corpus_info["file_format"])
        filenames = []
        for prefix in corpus_info["file_prefixes"]:
            filenames.extend([f for f in _filenames if os.path.basename(f).startswith(prefix)])

        lock = [threading.Lock()]  # make list to use it as reference in functools.partial
        tasks_completed = [0]  # make list to use it as reference in functools.partial
        tasks_total = len(filenames)
        line_sep = itertools.repeat("\n")
        queue = collections.deque()

        _callback = functools.partial(_progress_callback, lock, tasks_completed, tasks_total)

        output_dir = os.path.dirname(output_path)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)

        with open(corpus_path, "rb") as fi, open(output_path, "w", encoding="utf-8") as fo:
            print(f"Extracting {msgspec_class} from {corpus_path}...")

            zipped_file = zipfile.ZipFile(fi)
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                for filename in filenames:
                    future = executor.submit(_read_msgspec_in_zipobj, zipped_file, filename=filename)
                    future.add_done_callback(_callback)
                    queue.append(future)
                    while _get_available_memory_ratio() > max_memory_ratio:
                        if len(queue) == 0:
                            break
                        if queue[0].done():
                            lines = [line for line in queue.popleft().result() if line]
                            fo.writelines(itertools.chain.from_iterable(zip(lines, line_sep)))
                            fo.flush()
            while len(queue) > 0:
                if queue[0].done():
                    lines = [line for line in queue.popleft().result() if line]
                    fo.writelines(itertools.chain.from_iterable(zip(lines, line_sep)))
                    fo.flush()

        print(f"Extraction complete. Output saved to {output_path}")
