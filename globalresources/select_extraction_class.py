from globalresources.extraction_classes import *


class SelectClassExtraction:

    def __init__(self, conf: dict):
        self.conf = conf

    @log.logs
    def get_class(self):
        """
        Redireciona o arquivo lido a classe certa para realizar a extracao
        :return: a tabela extraida pela classe
        """
        fmsg = f'{SelectClassExtraction.__name__}.{self.get_class.__name__}'
        logger = log.createLogger(fmsg)
        # Getting the extractor class
        extraction_class = self.conf.get('class', 'CsvExcelExtractor')
        check_class = globals().get(extraction_class)
        # Checking if the class exists
        if not check_class or not isinstance(check_class, type):
            logger.error(f'A classe {extraction_class} nao existe')
            raise NotImplementedError(f'A classe {extraction_class} nao existe')
        return check_class
