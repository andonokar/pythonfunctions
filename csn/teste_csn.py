import yaml
from globalresources.extraction_classes import CsvExcelExtractor
with open('csn.yaml', 'r') as file:
    conf = yaml.safe_load(file)
escrita_conf = conf['escrita']

"""DEMANDA OK"""
# file_conf = conf['DEMANDA']
# key = "C:/Users/Anderson/Downloads/Demanda JAN_A_NOV_22.xlsx"
# extract = CsvExcelExtractor().prepara_tabela(key, key, file_conf)
# print(extract[0]['df'].columns)
# print(extract[0]['df'])

"""CARTEIRA OK"""
# file_conf = conf['CARTEIRA']
# key = "C:/Users/Anderson/Downloads/Carteira_02.09.2022_SEM PO OUT.xlsx"
# extract = CsvExcelExtractor().prepara_tabela(key, key, file_conf)
# print(extract[0]['df'].columns)
# print(extract[0]['df'])

"""FATURA OK OBS: LAMBDA NAO VAI PROCESSAR ARQUIVO DE 50MB"""
# file_conf = conf['FATURA']
# key = "C:/Users/Anderson/Downloads/Faturamento_JAN_A_AGO_COMBO_PA_OK.xlsx"
# extract = CsvExcelExtractor().prepara_tabela(key, key, file_conf)
# print(extract[0]['df'].columns.tolist())
# print(extract[0]['df']['data_faturamento'])

"""ESTOQUE OK OBS: LAMBDA NAO VAI PROCESSAR ARQUIVO DE 10MB"""
# file_conf = conf['ESTOQUE']
# key = "C:/Users/Anderson/Downloads/Estoque Formatado 01.09.2022tudo.xlsx"
# extract = CsvExcelExtractor().prepara_tabela(key, key, file_conf)
# print(extract[0]['df'].columns.tolist())
# print(extract[0]['df'])
