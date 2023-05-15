from typing import List

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

# key(integer) is dataSetSn parameter of aihub.or.kr/aihubdata/data/view.do
_CORPUS_INFO = {
    86: {  # 감성 대화 말뭉치
        "dir_name": "018.감성대화",
        "compressed_format": "zip",
        "file_patterns": [
            "**/라벨링데이터/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structures": [
            ".[]|.talk.content|.[]",
        ],
    },
    # 87: {  # 고객 응대 음성
    #     "dir_name": "고객 응대 음성",
    #     "compressed_format": "zip",
    #     "file_patterns": [
    #         "**/[[]라벨[]]*.zip",
    #     ],
    #     "file_format": "txt",
    #     "file_encoding": "utf-8",
    #     "data_structures": [
    #         ".[]",
    #     ],
    # },
    89: {  # 기계독해
        "dir_name": "기계독해",
        "compressed_format": "zip",
        "file_patterns": [
            "**/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structures": [
            ".data[]|.paragraphs[]|.context",
            ".data[]|.paragraphs[]|.qas[]|.question",
            ".data[]|.paragraphs[]|.qas[]|.answers[]|.text",
        ],
    },
    90: {  # 논문자료 요약
        "dir_name": "논문자료 요약",
        "compressed_format": "zip",
        "file_patterns": [
            "**/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structures": [
            ".data[]|.title",
            ".data[]|.summary_entire[]|.original_text",
            ".data[]|.summary_entire[]|.summary_text",
            ".data[]|.summary_section[]|.original_text",
            ".data[]|.summary_section[]|.summary_text",
        ],
    },
    92: {  # 도서자료 기계독해
        "dir_name": "도서자료 기계독해",
        "compressed_format": "zip",
        "file_patterns": [
            "**/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structures": [
            ".data[]|.title",
            ".data[]|.agency",
            ".data[]|.paragraphs[]|.context",
            ".data[]|.paragraphs[]|.qas[]|.question",
            ".data[]|.paragraphs[]|.qas[]|.answers[]|.text",
        ],
    },
    93: {  # 도서자료 요약
        "dir_name": "도서자료 요약",
        "compressed_format": "zip",
        "file_patterns": [
            "**/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structures": [
            ".metadata.doc_name",
            ".chapter",
            ".passage",
            ".summary",
        ],
    },
    94: {  # 명령어 음성(노인남여)
        "dir_name": "명령어 음성(노인남녀)",
        "compressed_format": "zip",
        "file_patterns": [
            "**/[[]라벨[]]*.zip",  # wrap '[', ']' with [] to avoid glob pattern
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structures": [
            ".전사정보.LabelText",
        ],
    },
    95: {  # 명령어 음성(소아,유아)
        "dir_name": "명령어 음성(소아,유아)",
        "compressed_format": "zip",
        "file_patterns": [
            "**/[[]라벨[]]*.zip",  # wrap '[', ']' with [] to avoid glob pattern
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structures": [
            ".전사정보.LabelText",
        ],
    },
    96: {  # 명령어 음성(일반남여)
        "dir_name": "명령어 음성(일반남녀)",
        "compressed_format": "zip",
        "file_patterns": [
            "**/[[]라벨[]]*.zip",  # wrap '[', ']' with [] to avoid glob pattern
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structures": [
            ".전사정보.LabelText",
        ],
    },
    # # deprecated version
    # 97: {  # 문서요약 텍스트
    #     "dir_name": "문서요약 텍스트",
    #     "compressed_format": "zip",
    #     "file_patterns": [
    #         "**/*.json.zip",
    #     ],
    #     "file_format": "json",
    #     "file_encoding": "utf-8",
    #     "data_structures": [
    #         ".[]|.article_original[]",
    #         ".[]|.abstractive",
    #     ],
    # },
    97: {  # 문서요약 텍스트
        "dir_name": "문서요약 텍스트",
        "compressed_format": "zip",
        "file_patterns": [
            "Training/*.zip",
            "Validation/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structures": [
            ".documents[]|.title",
            ".documents[]|.abstractive[]",
            ".documents[]|.text[]|.[]|.sentence",
        ],
    },
    # # field_names are non-ascii (not suitable for identifier)
    # 98: {  # 민원(콜센터) 질의-응답 데이터
    #     "dir_name": "민원(콜센터) 질의-응답 데이터",
    #     "compressed_format": "zip",
    #     "file_patterns": [
    #         "**/[[]라벨[]]*.zip",  # wrap '[', ']' with [] to avoid glob pattern
    #     ],
    #     "file_format": "json",
    #     "file_encoding": "utf-8",
    #     "data_structures": [
    #         ".[]|.고객질문(요청)",
    #         ".[]|.상담사질문(요청)",
    #         ".[]|.고객답변",
    #         ".[]|.상담사답변",
    #     ],
    # },
    99: {  # 법률 지식베이스
        "dir_name": "법률 지식베이스",
        "compressed_format": "zip",
        "file_patterns": [
            "**/*(Json).zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structures": [
            ".documents[]|.title",
            ".documents[]|.abstractive[]",
            ".documents[]|.text[]|.[]|.sentence",
        ],
    },
    463: {  # 방송 콘텐츠 대화체 음성인식 데이터
        "dir_name": "001.방송 콘텐츠 대화체 음성인식 데이터",
        "compressed_format": "zip",
        "file_patterns": [
            "01.데이터/1.Training/라벨링데이터/*.zip",
            "01.데이터/2.Validation/라벨링데이터/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structures": [
            ".utterance[]|.original_form",
        ],
    },
    464: {  # 주요 영역별 회의 음성인식 데이터
        "dir_name": "002.주요 영역별 회의 음성인식 데이터",
        "compressed_format": "zip",
        "file_patterns": [
            "01.데이터/1.Training/라벨링데이터/*.zip",
            "01.데이터/2.Validation/라벨링데이터/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": [
            ".utterance[]|.original_form",
        ],
    },
}


class AIHubExtractor(ZippedJsonExtractor):
    def __init__(self):
        self.corpus_info = _CORPUS_INFO
        super().__init__()

    def _get_corpus_info_by_path(self, corpus_path: str) -> dict:
        for corpus_info in self.corpus_info.values():
            corpus_path = os.path.normpath(corpus_path)
            if os.path.basename(corpus_path) == corpus_info["dir_name"]:
                return corpus_info
        raise ValueError("corpus_path is not valid")

    def extract(
        self, corpus_path: str, output_path: str, num_workers: int = os.cpu_count(), max_memory_ratio: float = 0.5
    ):
        def _read_msgspec_in_zipobj(zipobj, filename):
            data = []
            try:
                with zipobj.open(filename) as fj:
                    _data = msgspec.json.decode(fj.read(), type=self.msgspec_classes[0])
                    _data = self.flatten_data(_data)
                    data = _data
            except msgspec.ValidationError as e:
                print(f"msgspec.ValidationError: {filename} in {zipobj.filename}")
                print(e)
            except msgspec.DecodeError as e:
                print(f"msgspec.DecodeError: {filename} in {zipobj.filename}")
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

        corpus_info = self._get_corpus_info_by_path(corpus_path)

        with open(output_path, "w", encoding="utf-8") as fo:
            for data_structure in corpus_info["data_structures"]:
                print(f"Extracting {data_structure} from {corpus_path}...")
                self.create_msgspec_classes(data_structure)

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
                                        fo.writelines(itertools.chain.from_iterable(zip(lines, line_sep)))
                                        fo.flush()
                        while len(queue) > 0:
                            if queue[0].done():
                                lines = [line for line in queue.popleft().result() if line]
                                fo.writelines(itertools.chain.from_iterable(zip(lines, line_sep)))
                                fo.flush()
