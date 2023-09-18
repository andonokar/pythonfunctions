import globalresources.extraction_classes as extc


class SelectClassExtraction:
    depara = {
        'CsvExcelExtractor': extc.CsvExcelExtractor
    }

    def __init__(self, conf: dict):
        self.conf = conf

    def get_class(self):
        """
        Redireciona o arquivo lido a classe certa para realizar a extracao
        :return: a tabela extraida pela classe
        """
        # Getting the extractor class
        extraction_class = self.conf.get('class', 'CsvExcelExtractor')
        check_class = self.depara.get(extraction_class)
        # Checking if the class exists
        if not check_class or not isinstance(check_class, type):
            raise NotImplementedError(f'A classe {extraction_class} nao existe')
        return check_class
