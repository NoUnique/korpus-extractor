from typing import List, Optional

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

# raw corpus only
_CORPUS_INFO = {
    "spoken": {  # 구어 말뭉치
        "file_names": ["NIKL_SPOKEN_v1.1", "NIKL_SPOKEN_v1.2"],
        "compressed_format": "zip",
        "file_prefixes": ["SARW", "SBRW", "SERW"],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".document[]|.utterance[]|.form",
    },
    "written": {  # 문어 말뭉치
        "file_names": ["NIKL_WRITTEN_v1.0", "NIKL_WRITTEN_v1.1", "NIKL_WRITTEN_v1.2"],
        "compressed_format": "zip",
        "file_prefixes": ["WARW", "WBRW", "WCRW", "WZRW"],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".document[]|.paragraph[]|.form",
    },
    "newspaper": {  # 신문 말뭉치
        "file_names": ["NIKL_NEWSPAPER_v2.0"],
        "compressed_format": "zip",
        "file_prefixes": ["NWRW", "NLRW", "NPRW", "NIRW", "NZRW"],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".document[]|.paragraph[]|.form",
    },
    "newspaper_2020": {  # 신문 말뭉치 2020
        "file_names": ["NIKL_NEWSPAPER_2020_v1.0", "NIKL_NEWSPAPER_2020_v1.1"],
        "compressed_format": "zip",
        "file_prefixes": ["NWRW", "NLRW", "NPRW", "NIRW", "NZRW"],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".document[]|.paragraph[]|.form",
    },
    "newspaper_2021": {  # 신문 말뭉치 2021
        "file_names": ["NIKL_NEWSPAPER_2021_v1.0"],
        "compressed_format": "zip",
        "file_prefixes": ["NWRW", "NLRW", "NPRW", "NIRW", "NZRW"],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".document[]|.paragraph[]|.form",
    },
    "newspaper_2022": {  # 신문 말뭉치 2022
        "file_names": ["NIKLNEWSPAPER_2022_v1.0"],
        "compressed_format": "zip",
        "file_prefixes": ["NWRW", "NLRW", "NPRW", "NIRW", "NZRW"],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".document[]|.paragraph[]|.form",
    },
    "korean_parliamentary_2021": {  # 국회 회의록 말뭉치 2021
        "file_names": ["NIKL_KParlty_2021_v1.0"],
        "compressed_format": "zip",
        "file_prefixes": ["SBRW"],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".document.utterance[]|.form",
    },
    "messenger": {  # 메신저 말뭉치
        "file_names": ["NIKL_MESSENGER_v2.0"],
        "compressed_format": "zip",
        "file_prefixes": ["MDRW", "MMRW"],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".document[]|.utterance[]|.form",
        "special_tags": [
            "{emoji}",  # 이모지
            "{system: gift}",  # 선물하기
            "{system: call}",  # 통화
            "{system: money}",  # 송금
            "{system: notice}",  # 공지
            "{system: map}",  # 지도 공유
            "{system: contact}",  # 연락처 공유
            "{system: delete}",  # 메시지 삭제
            "{share:photo}",  # 사진 공유
            "{share:videoclip}",  # 동영상 공유
            "{share:music}",  # 음악 공유
            "{share:file}",  # 파일 공유
            "{share:url}",  # URL 공유
            "{share:info}",  # 정보 공유
        ],
        "deidentified_tags": [
            "&name&",  # 이름
            "&social-security-num&",  # 주민등록번호
            "&card-num&",  # 카드번호
            "&address&",  # 주소
            "&tel-num&",  # 전화번호
            "&account&",  # 계좌번호
            "&num&",  # 기타 번호
            "&affiliation&",  # 출신, 소속
            "&other&",  # 기타
        ],
    },
    "online_text_message_2021": {  # 온라인 대화 말뭉치 2021
        "file_names": ["NIKL_OM_2021_v1.0"],
        "compressed_format": "zip",
        "file_prefixes": ["MDRW", "MMRW"],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".document[]|.utterance[]|.form",
        "special_tags": [
            "{emoji}",  # 이모지
            "{system: gift}",  # 선물하기
            "{system: call}",  # 통화
            "{system: money}",  # 송금
            "{system: notice}",  # 공지
            "{system: map}",  # 지도 공유
            "{system: contact}",  # 연락처 공유
            "{system: delete}",  # 메시지 삭제
            "{share:photo}",  # 사진 공유
            "{share:video}",  # 동영상 공유
            "{share:music}",  # 음악 공유
            "{share:file}",  # 파일 공유
            "{share:voice}",  # 음성 메시지 공유
            "{share:info}",  # 정보 공유
            "{share:url}",  # URL 공유
            "{censored}",  # 부적절 대화
        ],
        "deidentified_tags": [
            "&name&",  # 이름
            "&account&",  # 온라인 계정
            "&social-security-num&",  # 고유 식별 번호
            "&tel-num&",  # 전화번호
            "&card-num&",  # 금융 번호
            "&num&",  # 기타 번호
            "&address&",  # 주소
            "&affiliation&",  # 출신, 소속
            "&others&",  # 기타
        ],
    },
    "dialogue_2020": {  # 일상 대화 말뭉치 2020
        "file_names": ["NIKL_DIALOGUE_2020_v1.2", "NIKL_DIALOGUE_2020_v1.3"],
        "compressed_format": "zip",
        "file_prefixes": ["SDRW"],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".document[]|.utterance[]|.form",
        "special_tags": [
            "{laughing}",  # 웃음
            "{clearing}",  # 목청 가다듬는 소리
            "{singing}",  # 노래
            "{applausing}",  # 박수
            "((추정 전사))",  # 잘 들리지 않는 부분
            "((xx))",  # 들리지 않는 음절
            "(())",  # 전혀 들리지 않는 qnqns
            "~",  # 담화 표지
            "-불완전 발화-",  # 불완전 발화
        ],
        "deidentified_tags": [
            "&name&",  # 이름
            "&social-security-num&",  # 주민등록번호
            "&card-num&",  # 카드번호
            "&address&",  # 주소
            "&tel-num&",  # 전화번호
            "&company-name&",  # 상호명
        ],
    },
    "dialogue_2021": {  # 일상 대화 말뭉치 2021
        "file_names": ["NIKL_DIALOGUE_2021_v1.0"],
        "compressed_format": "zip",
        "file_prefixes": ["SDRW"],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".document[]|.utterance[]|.form",
        "special_tags": [
            "{laughing}",  # 웃음
            "{clearing}",  # 목청 가다듬는 소리
            "{singing}",  # 노래
            "{applausing}",  # 박수
            "((추정 전사))",  # 잘 들리지 않는 부분
            "((xx))",  # 들리지 않는 음절
            "(())",  # 전혀 들리지 않는 qnqns
            "~",  # 담화 표지
            "-불완전 발화-",  # 불완전 발화
        ],
        "deidentified_tags": [
            "&name&",  # 이름
            "&social-security-num&",  # 주민등록번호
            "&card-num&",  # 카드번호
            "&address&",  # 주소
            "&tel-num&",  # 전화번호
            "&company-name&",  # 상호명
        ],
    },
    "korean-foreign_language_parallel_2021": {  # 한국어-외국어 병렬 말뭉치 2021
        "file_names": ["NIKL_PA_2021_v1.0"],
        "compressed_format": "zip",
        "file_prefixes": ["NIORPAKOVI", "WZORPAKOVI", "SEORPAKOVI", "SDORPAKOVI"],  # Ko-Vi only
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": ".parallel[]|.source",
    },
}


class ModuExtractor(ZippedJsonExtractor):
    def __init__(self):
        super().__init__()
        self.corpus_info = _CORPUS_INFO

    def _get_corpus_info_by_path(self, corpus_path: str) -> dict:
        for corpus_info in self.corpus_info.values():
            filename = re.sub(
                f".{corpus_info['compressed_format']}$",
                "",
                os.path.basename(corpus_path),
            )
            if filename in corpus_info["file_names"]:
                return corpus_info
        raise ValueError(f"Corpus path {corpus_path} is not valid.")

    def extract(
        self,
        corpus_path: str,
        output_path: str,
        num_workers: Optional[int] = os.cpu_count(),
        max_memory_ratio: float = 0.5,
    ):
        corpus_info = self._get_corpus_info_by_path(corpus_path)
        _filenames = self.read_filenames_in_zip(corpus_path, extension=corpus_info["file_format"])
        filenames = []
        for prefix in corpus_info["file_prefixes"]:
            filenames.extend([f for f in _filenames if os.path.basename(f).startswith(prefix)])
        self.create_msgspec_classes(corpus_info["data_structure"])

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
            lock: List[threading.Lock], tasks_completed: List[int], tasks_total: int, _: concurrent.futures.Future
        ) -> None:
            indexes = [int(tasks_total * (i / 10)) for i in range(1, 11)]
            with lock[0]:
                tasks_completed[0] += 1
                percent = tasks_completed[0] / tasks_total * 100
                line_end = "\n" if tasks_completed[0] in indexes else "\r"
                print(f"{tasks_completed[0]:#5d} / {tasks_total} ( {percent:5.2f} %) completed", end=line_end)

        lock = [threading.Lock()]  # make list to use it as reference in functools.partial
        tasks_completed = [0]  # make list to use it as reference in functools.partial
        tasks_total = len(filenames)
        line_sep = itertools.repeat("\n")
        queue = collections.deque()

        _callback = functools.partial(_progress_callback, lock, tasks_completed, tasks_total)

        with open(corpus_path, "rb") as fi, open(output_path, "w", encoding="utf-8") as fo:
            zipped_file = zipfile.ZipFile(fi)
            with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
                for filename in filenames:
                    future = executor.submit(
                        _read_msgspec_in_zipobj,
                        zipped_file,
                        filename=filename,
                    )
                    future.add_done_callback(_callback)
                    queue.append(future)
                    while _get_available_memory_ratio() > max_memory_ratio:
                        if len(queue) == 0:
                            break
                        if queue[0].done():
                            lines = queue.popleft().result()
                            fo.writelines(itertools.chain.from_iterable(zip(lines, line_sep)))
                            fo.flush()
            while len(queue) > 0:
                if queue[0].done():
                    lines = queue.popleft().result()
                    fo.writelines(itertools.chain.from_iterable(zip(lines, line_sep)))
                    fo.flush()
