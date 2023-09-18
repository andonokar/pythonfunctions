from globalresources.process_dataframe import ProcessDataFrame


class StrategyExtractor:
    """
    strategy pattern para as classes de extracao
    """
    def __init__(self, extrator: type, processdf: ProcessDataFrame):
        self.extrator = extrator
        self.processdf = processdf

    def extrair_para_avro(self) -> list[dict]:
        tabelas = self.extrator().prepara_tabela(self.processdf)
        return tabelas
