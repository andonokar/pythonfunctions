from globalresources.process_dataframe import ProcessDataFrame


class CriaDataFrame:
    def __init__(self, extrator: type, processdf: ProcessDataFrame):
        self.extrator = extrator
        self.processdf = processdf

    def extrair_para_avro(self):
        tabelas = self.extrator().prepara_tabela(self.processdf)
        return tabelas
