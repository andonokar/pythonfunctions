class CriaDataFrame:
    def __init__(self, extrator, file, key, conf):
        self.extrator = extrator
        self.key = key
        self.file = file
        self.conf = conf

    def extrair_para_avro(self):
        tabelas = self.extrator().prepara_tabela(self.file, self.key, self.conf)
        return tabelas
