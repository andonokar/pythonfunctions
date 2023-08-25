# import yaml
# from avro.schema import parse
# import json
# with open("C:/Users/Anderson/Downloads/telecom.yaml", 'r') as r:
#     config = yaml.safe_load(r)
#
# teste = config['rbx_ContratosMotivos']
# print(teste)
# csv_options = teste.get('csv')
# print(csv_options)
# header = csv_options.get('header')
# print(header)

def func(**kwargs):
    data = kwargs
    a = {'a': 1}
    data = {**data, **a}
    print(data)


func(ronaldo=3)
