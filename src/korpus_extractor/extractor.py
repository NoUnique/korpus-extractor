from abc import ABC, abstractmethod


class Extractor(ABC):
    @abstractmethod
    def extract(self, corpus_path: str, output_path: str):
        pass
