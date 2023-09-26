from globalresources.treated_dataframe import TreatedDataFrame
from typing import Protocol


class ProcessDataFrame(Protocol):
    pass


class Extractor(Protocol):
    def prepara_tabela(self, pdf: ProcessDataFrame) -> list[TreatedDataFrame]:
        pass


class StrategyExtractor:
    """
    strategy pattern para as classes de extracao
    """
    def __init__(self, extrator: Extractor, processdf: ProcessDataFrame):
        self.extrator = extrator
        self.processdf = processdf

    def extrair_para_avro(self) -> list[TreatedDataFrame]:
        tabelas = self.extrator.prepara_tabela(self.processdf)
        return tabelas
