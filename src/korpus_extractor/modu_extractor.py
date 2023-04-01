import os
import re

from .extractor import Extractor

# raw corpus only
_CORPUS_INFO = {
    "spoken": {  # 구어 말뭉치
        "file_names": ["NIKL_SPOKEN_v1.1", "NIKL_SPOKEN_v1.2"],
        "compressed_format": "zip",
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": "document|[*].utterance|[*].form",
    },
    "written": {  # 문어 말뭉치
        "file_names": ["NIKL_WRITTEN_v1.0", "NIKL_WRITTEN_v1.1", "NIKL_WRITTEN_v1.2"],
        "compressed_format": "zip",
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": "document|[*].paragraph|[*].form",
    },
    "newspaper": {  # 신문 말뭉치
        "file_names": ["NIKL_NEWSPAPER_v2.0"],
        "compressed_format": "zip",
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": "document|[*].paragraph|[*].form",
    },
    "newspaper_2020": {  # 신문 말뭉치 2020
        "file_names": ["NIKL_NEWSPAPER_2020_v1.0", "NIKL_NEWSPAPER_2020_v1.1"],
        "compressed_format": "zip",
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": "document|[*].paragraph|[*].form",
    },
    "newspaper_2021": {  # 신문 말뭉치 2021
        "file_names": ["NIKL_NEWSPAPER_2021_v1.0"],
        "compressed_format": "zip",
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": "document|[*].paragraph|[*].form",
    },
    "newspaper_2022": {  # 신문 말뭉치 2022
        "file_names": ["NIKLNEWSPAPER_2022_v1.0"],
        "compressed_format": "zip",
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": "document|[*].paragraph|[*].form",
    },
    "korean_parliamentary_2021": {  # 국회 회의록 말뭉치 2021
        "file_names": ["NIKL_KPalrty_2021_v1.0"],
        "compressed_format": "zip",
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": "document.utterance|[*].form",
    },
    "messenger": {  # 메신저 말뭉치
        "file_names": ["NIKL_MESSENGER_v2.0"],
        "compressed_format": "zip",
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": "document|[*].utterance|[*].form",
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
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": "document|[*].utterance|[*].form",
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
    "daily_conversation_2020": {  # 일상 대화 말뭉치 2020
        "file_names": ["NIKL_DIALOGUE_2020_v1.2", "NIKL_DIALOGUE_2020_v1.3"],
        "compressed_format": "zip",
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": "document|[*].utterance|[*].form",
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
    "daily_conversation_2021": {  # 일상 대화 말뭉치 2021
        "file_names": ["NIKL_DIALOGUE_2021_v1.0"],
        "compressed_format": "zip",
        "file_format": "json",
        "file_encoding": "utf-8",
        "data_structure": "document|[*].utterance|[*].form",
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
}


class ModuExtractor(Extractor):
    def __init__(self):
        self.corpus_info = _CORPUS_INFO

    def _get_corpus_info_by_path(self, corpus_path: str) -> dict:
        for corpus_name, corpus_info in self.corpus_info.items():
            filename = re.sub(
                f".{corpus_info['compressed_format']}$",
                "",
                os.path.basename(corpus_path),
            )
            if filename in corpus_info["file_names"]:
                return corpus_info
        return None

    def extract(self, corpus_path: str, output_path: str):
        corpus_info = self._get_corpus_info_by_path(corpus_path)
        file_names = self.corpus_info["file_names"]
