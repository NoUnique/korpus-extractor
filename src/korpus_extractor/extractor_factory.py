from .extractor import Extractor
from .modu_extractor import ModuExtractor

_EXTRACTORS = {
    "modu": ModuExtractor,
}


class ExtractorFactory:
    def __init__(self):
        self.extractors = _EXTRACTORS

    def get_extractor(self, corpus_source: str) -> Extractor:
        if corpus_source in self.extractors:
            return self.extractors[corpus_source]()
        else:
            raise ValueError("Invalid corpus source")
