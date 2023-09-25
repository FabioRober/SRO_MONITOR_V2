from typing import Dict

URL_BUSCA_SRO_MONITOR = 'https://sromonitor.correios.com.br/app/desempenho-qualidade-sintetico/pesquisa-unidade.php'
URL_BUSCA_OBJETOS_REGIONAL = 'https://sromonitor.correios.com.br/app/desempenho-qualidade-sintetico/pesquisa-objeto.php'
URL_BUSCA_SRO_MONITOR_REGIONAL_UNIDADES = 'https://sromonitor.correios.com.br/app/desempenho-qualidade-pendencia-baixa/pesquisa-unidade.php'
URL_BUSCA_SRO_MONITOR_REGIONAL_ONJETOS = 'https://sromonitor.correios.com.br/app/desempenho-qualidade-sintetico/pesquisa-objeto.php'

DICIONARIO_PESQUISA = {
    'Codigo_SE': '',
    'Tipo_Unidade': '',
    'Nome_Unidade': '',
    'Data_Inicial': '',
    'Data_Final': '',
    'Detalhar_Unidade': ''
}

DICIONARIO_PESQUISA_UNIDADES = {
    'Tipo_Lista': '',
    'Codigo_SE': '64',
    'Numero_REOP.': '',
    'Tipo_Unidade': '',
    'Nome_Unidade': '',
}

DICIONARIO_PESQUISA_OBJETOS = {
    'Codigo_SRO': '',
    'Tipo_Lista': '',
}

TIPOS_DE_LISTA = ['OEC', 'LDI']