from typing import List, Literal, Optional

import collections
import concurrent.futures
import functools
import glob
import itertools
import os
import threading
import zipfile

import msgspec
import psutil

from .extractor import ZippedJsonExtractor


class AIHubExtractor(ZippedJsonExtractor):
    def __init__(self, config=None):
        if config:
            if os.path.exists(config):
                self.corpus_info = {config: self._load_config(config)}
            else:
                # config is dataSetSn parameter of aihub.or.kr/aihubdata/data/view.do
                default_config_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), "configs", "aihub", f"{config}.yaml"
                )
                if os.path.exists(default_config_path):
                    self.corpus_info = {config: self._load_config(default_config_path)}
                else:
                    raise FileNotFoundError(f"Config file not found: {default_config_path}")
        else:
            self.corpus_info = {}
            config_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "configs", "aihub")
            for yaml_file in os.listdir(config_dir):
                if yaml_file.endswith(".yaml"):
                    # filename (without .yaml) is dataSetSn parameter of aihub.or.kr/aihubdata/data/view.do
                    config_name = yaml_file[:-5]
                    config_path = os.path.join(config_dir, yaml_file)
                    try:
                        self.corpus_info[config_name] = self._load_config(config_path)
                    except Exception as e:
                        print(f"Error loading {config_path}: {e}")
        super().__init__()

    def _get_corpus_info_by_path(self, corpus_path: str) -> dict:
        for corpus_info in self.corpus_info.values():
            if corpus_info is None:
                continue
            corpus_path = os.path.normpath(corpus_path)
            if os.path.basename(corpus_path) == corpus_info["dir_name"]:
                return corpus_info
        raise ValueError(f"corpus_path '{corpus_path}' is not valid. No matching configuration found.")

    def extract(
        self,
        corpus_path: str,
        output_path: str,
        extraction_type: Literal["sentence", "document"] = "sentence",
        num_workers: Optional[int] = os.cpu_count(),
        max_memory_ratio: float = 0.5,
        size_limit: Optional[str] = None,
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
                    if corpus_info["file_encoding"] != "utf-8":
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
            lock: List[threading.Lock],
            files_completed: int,
            files_total: int,
            tasks_completed: List[int],
            tasks_total: int,
            _: concurrent.futures.Future,
        ) -> None:
            indexes = [int(tasks_total * (i / 10)) for i in range(1, 11)]
            with lock[0]:
                tasks_completed[0] += 1
                files_percent = files_completed / files_total * 100
                percent = tasks_completed[0] / tasks_total * 100
                line_end = "\n" if tasks_completed[0] in indexes else "\r"
                print(
                    f"{files_completed:#5d} / {files_total} ({files_percent:6.2f} %) files, {tasks_completed[0]:#5d} / {tasks_total} ({percent:6.2f} %) completed",
                    end=line_end,
                )

        # Parse size limit
        max_file_size = self.parse_size_limit(size_limit) if size_limit else None
        
        if max_file_size:
            # Split file mode
            current_file_index = 1
            current_file_size = 0
            current_output_path = self.get_split_file_path(output_path, current_file_index)
            fo = open(current_output_path, "w", encoding="utf-8")
        else:
            # Single file mode
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir, exist_ok=True)
            fo = open(output_path, "w", encoding="utf-8")

        try:
            print(f"Extracting {msgspec_class} from {corpus_path}...")

            zipfile_paths = []
            for file_pattern in corpus_info["file_patterns"]:
                zipfile_paths.extend(glob.glob(os.path.join(corpus_path, file_pattern), recursive=True))

            for i, zipfile_path in enumerate(zipfile_paths, start=1):
                filenames = self.read_filenames_in_zip(zipfile_path, extension=corpus_info["file_format"])

                lock = [threading.Lock()]  # make list to use it as reference in functools.partial
                files_completed = i
                files_total = len(zipfile_paths)
                tasks_completed = [0]  # make list to use it as reference in functools.partial
                tasks_total = len(filenames)
                line_sep = itertools.repeat("\n")
                queue = collections.deque()

                _callback = functools.partial(
                    _progress_callback, lock, files_completed, files_total, tasks_completed, tasks_total
                )

                with open(zipfile_path, "rb") as fi:
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
                                    for line in lines:
                                        line_with_sep = line + "\n"
                                        line_bytes = len(line_with_sep.encode('utf-8'))
                                        
                                        if max_file_size and current_file_size + line_bytes > max_file_size:
                                            # Close current file and open new one
                                            fo.close()
                                            current_file_index += 1
                                            current_file_size = 0
                                            current_output_path = self.get_split_file_path(output_path, current_file_index)
                                            fo = open(current_output_path, "w", encoding="utf-8")
                                        
                                        fo.write(line_with_sep)
                                        if max_file_size:
                                            current_file_size += line_bytes
                                    fo.flush()
                    while len(queue) > 0:
                        if queue[0].done():
                            lines = [line for line in queue.popleft().result() if line]
                            for line in lines:
                                line_with_sep = line + "\n"
                                line_bytes = len(line_with_sep.encode('utf-8'))
                                
                                if max_file_size and current_file_size + line_bytes > max_file_size:
                                    # Close current file and open new one
                                    fo.close()
                                    current_file_index += 1
                                    current_file_size = 0
                                    current_output_path = self.get_split_file_path(output_path, current_file_index)
                                    fo = open(current_output_path, "w", encoding="utf-8")
                                
                                fo.write(line_with_sep)
                                if max_file_size:
                                    current_file_size += line_bytes
                            fo.flush()

        finally:
            fo.close()

        if max_file_size:
            print(f"Extraction complete. Output saved to {os.path.dirname(self.get_split_file_path(output_path, 1))} ({current_file_index} files)")
        else:
            print(f"Extraction complete. Output saved to {output_path}")
