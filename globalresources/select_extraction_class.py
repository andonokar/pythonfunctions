import globalresources.extraction_classes as extc


class SelectClassExtraction:
    depara = {
        'CsvExcelExtractor': extc.CsvExcelExtractor
    }

    def get_class(self, conf: dict) -> extc.Extrator:
        """
        Redireciona o arquivo lido a classe certa para realizar a extracao
        :return: a tabela extraida pela classe
        """
        # Getting the extractor class
        extraction_class = conf.get('class', 'CsvExcelExtractor')
        check_class = self.depara.get(extraction_class)
        # Checking if the class exists
        if not check_class or not isinstance(check_class, type):
            raise NotImplementedError(f'A classe {extraction_class} nao existe')
        return check_class()
