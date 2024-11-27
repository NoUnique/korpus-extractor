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
        "sentence_extraction": [
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
    #     "sentence_extraction": [
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
        "sentence_extraction": [
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
        "sentence_extraction": [
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
        "sentence_extraction": [
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
        "sentence_extraction": [
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
        "sentence_extraction": [
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
        "sentence_extraction": [
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
        "sentence_extraction": [
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
    #     "sentence_extraction": [
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
        "sentence_extraction": [
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
    #     "sentence_extraction": [
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
        "sentence_extraction": [
            ".documents[]|.title",
            ".documents[]|.abstractive[]",
            ".documents[]|.text[]|.[]|.sentence",
        ],
    },
    110: {  # 전문분야 말뭉치
        "dir_name": "전문분야 말뭉치",
        "compressed_format": "zip",
        "file_patterns": [
            "Training/*.zip",
            "Validation/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": {
            "data": [
                {
                    "title": "string",
                    "rows": [
                        {
                            "text": "string",
                        }
                    ],
                }
            ]
        },
        "document_extraction": """
        for datum in root.data:
            document_content = ""
            if datum.title:
                document_content += f'{datum.title}\\n\\n'
            for row in datum.rows:
                document_content += f'{row.text}\\n'
            transformed_document = {"text": document_content.strip()}
            transformed_documents.append(transformed_document)
        """,
        "sentence_extraction": [
            ".data[]|.rows[]|.text",
        ],
    },
    118: {  # 한국어 방언 발화(강원도)
        "dir_name": "한국어 방언 발화(강원도)",
        "compressed_format": "zip",
        "file_patterns": [
            "Training/[[]라벨[]]*.zip",
            "Validation/[[]라벨[]]*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": {
            "speaker": [
                {
                    "id": "string",
                }
            ],
            "utterance": [
                {
                    "form": "string",
                    "standard_form": "string",
                    "dialect_form": "string",
                    "speaker_id": "string",
                }
            ],
        },
        "document_extraction": """
        speaker_ids = {speaker.id: idx for idx, speaker in enumerate(root.speaker, start=1)}
        document_content = ""
        for utterance in root.utterance:
            document_content += f'P{speaker_ids[utterance.speaker_id]}: {utterance.standard_form}\\n'
        transformed_document = {"text": document_content.strip()}
        transformed_documents.append(transformed_document)
        """,
        "sentence_extraction": [
            ".utterance[]|.standard_form",
            ".utterance[]|.dialect_form",
        ],
    },
    119: {  # 한국어 방언 발화(경상도)
        "dir_name": "한국어 방언 발화(경상도)",
        "compressed_format": "zip",
        "file_patterns": [
            "Training/[[]라벨[]]*.zip",
            "Validation/[[]라벨[]]*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": {
            "speaker": [
                {
                    "id": "string",
                }
            ],
            "utterance": [
                {
                    "form": "string",
                    "standard_form": "string",
                    "dialect_form": "string",
                    "speaker_id": "string",
                }
            ],
        },
        "document_extraction": """
        speaker_ids = {speaker.id: idx for idx, speaker in enumerate(root.speaker, start=1)}
        document_content = ""
        for utterance in root.utterance:
            document_content += f'P{speaker_ids[utterance.speaker_id]}: {utterance.standard_form}\\n'
        transformed_document = {"text": document_content.strip()}
        transformed_documents.append(transformed_document)
        """,
        "sentence_extraction": [
            ".utterance[]|.standard_form",
            ".utterance[]|.dialect_form",
        ],
    },
    120: {  # 한국어 방언 발화(전라도)
        "dir_name": "한국어 방언 발화(전라도)",
        "compressed_format": "zip",
        "file_patterns": [
            "Training/[[]라벨[]]*.zip",
            "Validation/[[]라벨[]]*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": {
            "speaker": [
                {
                    "id": "string",
                }
            ],
            "utterance": [
                {
                    "form": "string",
                    "standard_form": "string",
                    "dialect_form": "string",
                    "speaker_id": "string",
                }
            ],
        },
        "document_extraction": """
        speaker_ids = {speaker.id: idx for idx, speaker in enumerate(root.speaker, start=1)}
        document_content = ""
        for utterance in root.utterance:
            document_content += f'P{speaker_ids[utterance.speaker_id]}: {utterance.standard_form}\\n'
        transformed_document = {"text": document_content.strip()}
        transformed_documents.append(transformed_document)
        """,
        "sentence_extraction": [
            ".utterance[]|.standard_form",
            ".utterance[]|.dialect_form",
        ],
    },
    121: {  # 한국어 방언 발화(제주도)
        "dir_name": "한국어 방언 발화(제주도)",
        "compressed_format": "zip",
        "file_patterns": [
            "Training/[[]라벨[]]*.zip",
            "Validation/[[]라벨[]]*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": {
            "speaker": [
                {
                    "id": "string",
                }
            ],
            "utterance": [
                {
                    "form": "string",
                    "standard_form": "string",
                    "dialect_form": "string",
                    "speaker_id": "string",
                }
            ],
        },
        "document_extraction": """
        speaker_ids = {speaker.id: idx for idx, speaker in enumerate(root.speaker, start=1)}
        document_content = ""
        for utterance in root.utterance:
            document_content += f'P{speaker_ids[utterance.speaker_id]}: {utterance.standard_form}\\n'
        transformed_document = {"text": document_content.strip()}
        transformed_documents.append(transformed_document)
        """,
        "sentence_extraction": [
            ".utterance[]|.standard_form",
            ".utterance[]|.dialect_form",
        ],
    },
    122: {  # 한국어 방언 발화(충청도)
        "dir_name": "한국어 방언 발화(충청도)",
        "compressed_format": "zip",
        "file_patterns": [
            "Training/[[]라벨[]]*.zip",
            "Validation/[[]라벨[]]*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": {
            "speaker": [
                {
                    "id": "string",
                }
            ],
            "utterance": [
                {
                    "form": "string",
                    "standard_form": "string",
                    "dialect_form": "string",
                    "speaker_id": "string",
                }
            ],
        },
        "document_extraction": """
        speaker_ids = {speaker.id: idx for idx, speaker in enumerate(root.speaker, start=1)}
        document_content = ""
        for utterance in root.utterance:
            document_content += f'P{speaker_ids[utterance.speaker_id]}: {utterance.standard_form}\\n'
        transformed_document = {"text": document_content.strip()}
        transformed_documents.append(transformed_document)
        """,
        "sentence_extraction": [
            ".utterance[]|.standard_form",
            ".utterance[]|.dialect_form",
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
        "sentence_extraction": [
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
        "sentence_extraction": [
            ".utterance[]|.original_form",
        ],
    },
    624: {  # 대규모 웹데이터 기반 한국어 말뭉치 데이터
        "dir_name": "030.웹데이터 기반 한국어 말뭉치 데이터",
        "compressed_format": "zip",
        "file_patterns": [
            "01.데이터/1.Training/원천데이터/*.zip",
            "01.데이터/2.Validation/원천데이터/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": {
            "SJML": {
                "text": [
                    {
                        "title": "string",
                        "subtitle": "string",
                        "content": "string",
                    },
                ],
            },
        },
        "document_extraction": """
        for txt in root.SJML.text:
            document_content = ""
            if txt.title.strip():
                document_content += f'{txt.title}\\n\\n'
            if txt.subtitle.strip():
                document_content += f'{txt.subtitle}\\n\\n'
            if txt.content.strip():
                document_content += f'{txt.content}\\n'
            transformed_document = {"text": document_content.strip()}
            transformed_documents.append(transformed_document)
        """,
        "sentence_extraction": [
            ".SJML.text[]|.title",
            ".SJML.text[]|.subtitle",
            ".SJML.text[]|.content",
        ],
    },
    653: {  # 대규모 구매도서 기반 한국어 말뭉치 데이터
        "dir_name": "029.대규모 구매도서 기반 한국어 말뭉치 데이터",
        "compressed_format": "zip",
        "file_patterns": [
            "01.데이터/1.Training/라벨링데이터/*.zip",
            "01.데이터/2.Validation/라벨링데이터/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": {
            "paragraphs": [
                {
                    "sentences": [
                        {
                            "text": "string",
                        }
                    ],
                }
            ],
        },
        "document_extraction": """
        for paragraph in root.paragraphs:
            document_content = ""
            for sentence in paragraph.sentences:
                document_content += f'{sentence.text}\\n'
            transformed_document = {"text": document_content.strip()}
            transformed_documents.append(transformed_document)
        """,
        "sentence_extraction": [
            ".paragraphs[]|.sentences[]|.text",
        ],
    },
    71343: {  # SNS 데이터 고도화
        "dir_name": "297.SNS 데이터 고도화",
        "compressed_format": "zip",
        "file_patterns": [
            "01-1.정식개방데이터/Training/01.원천데이터/*.zip",
            "01-1.정식개방데이터/Validation/01.원천데이터/*.zip",
        ],
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": {
            "body": [
                {
                    "participantID": "string",
                    "utterance": "string",
                }
            ]
        },
        "document_extraction": """
        document_content = ""
        for body in root.body:
            document_content += f'{body.participantID}:{body.utterance}\\n'
        transformed_document = {"text": document_content.strip()}
        transformed_documents.append(transformed_document)
        """,
        "sentence_extraction": [
            ".body[]|.utterance",
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
                    _data = msgspec.json.decode(fj.read(), type=msgspec_class)
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

        with open(output_path, "w", encoding="utf-8") as fo:
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
                                    fo.writelines(itertools.chain.from_iterable(zip(lines, line_sep)))
                                    fo.flush()
                    while len(queue) > 0:
                        if queue[0].done():
                            lines = [line for line in queue.popleft().result() if line]
                            fo.writelines(itertools.chain.from_iterable(zip(lines, line_sep)))
                            fo.flush()
