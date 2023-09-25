from src.model import Unidade, Calendario, ConexaoMySql, SroMonitor
from datetime import timedelta
from src.constantes import *
import pymysql.cursors
from tqdm import tqdm


import requests


class FabricaUnidades:

    def __init__(self):

        self.conexao = pymysql.connect(
            host='localhost',
            user='root',
            password='ect-2023',
            database=f'SRO_RS',
            cursorclass=pymysql.cursors.DictCursor)


    def monte_unidades(self, nome: str) -> None:
        print(nome)
        consulta = self.consulta_unidade_DB(nome)
        consulta = consulta[0]

        unidade_temp = Unidade(consulta.get('Gerencia'),
                               consulta.get('Unidade'),
                               consulta.get('SRO'),
                               consulta.get('Vinculacao'),
                               consulta.get('email'),
                               consulta.get('mcu'))
        return unidade_temp




    def consulta_unidade_DB(self, requisição: str):

        with self.conexao.cursor() as c:
            # od_sql = f"SELECT *FROM {nome_tabela} WHERE Unidade = {unidade};"
            #cod_sql = f"SELECT * FROM cadastro WHERE `SRO` LIKE '{requisição}'"
            cod_sql = f"SELECT * FROM cadastro WHERE `Unidade` LIKE '{requisição}'"
            c.execute(cod_sql)
            consulta = c.fetchall()
            # self.conexao.close()
            return consulta


class GerenciadorBancoDados:
    def __init__(self, nome_banco_dados):
        self.calendario = Calendario()
        self.lista_unidades_pendencias = []
        self.lista_unidade_criadas = []
        self.lista_final = []
        self.lista_unidade_ofensoras = []
        self.conexao = ConexaoMySql(nome_banco_dados)
        self.sro_monitor = SroMonitor()
        self.buscar_gravar_dados_sro_monitor_nacional()
        self.buscar_gravar_dados_sro_monitor_regional()
        self._buscar_unidades_pendentes()
        self.criar_unidades()
        self.retorne_resultado_objetos()
        self.arquivar_pendencia_objetos()




    def buscar_gravar_dados_sro_monitor_nacional(self) -> None:
        pesq_nacional = DICIONARIO_PESQUISA
        pesq_nacional['Data_Inicial'] = self.conexao.consulta_ultimo_registro_banco('SRO_RS')
        pesq_nacional['Data_Final'] = self.calendario.dia_ontem_barras
        consulta_sro_monitor = self.sro_monitor.retorne_sro_monitor(URL_BUSCA_SRO_MONITOR, pesq_nacional)

        retorno = self.calcula_col_pendencia_br(consulta_sro_monitor)
        self.conexao.arquivar_dados(retorno, 'ano_2023')

    @staticmethod
    def calcula_col_pendencia_br(arquivos: list) -> list:
        for arquivo in arquivos:
            carga_lancada = int(arquivo.get('cargaLancada'))
            carga_baixada = int(arquivo.get('cargaBaixada'))
            arquivo['pendencia'] = carga_lancada - carga_baixada
            arquivo['cargaLancada'] = int(arquivo.get('cargaLancada'))
            arquivo['cargaBaixada'] = int(arquivo.get('cargaBaixada'))

        return arquivos

    def buscar_gravar_dados_sro_monitor_regional(self) -> None:

        pesq_regional = DICIONARIO_PESQUISA
        pesq_regional['Codigo_SE'] = '64'

        pesq_regional['Data_Inicial'] = self.calendario.dia_ontem_relatorio_regional
        pesq_regional['Data_Final'] = self.calendario.dia_ontem_relatorio_regional
        self.conexao.deletar_dados_tabela_bd('pendencia_ofensoras')
        consulta_sro_monitor = self.sro_monitor.retorne_sro_monitor(URL_BUSCA_SRO_MONITOR, pesq_regional)
        retorno = self.calcula_col_pendencia_rs(consulta_sro_monitor)
        vincular: list = self.montar_unidade(retorno)
        self.conexao.arquivar_dados(vincular, 'pendencia_ofensoras')
        pass

    @staticmethod
    def calcula_col_pendencia_rs(arquivos: list) -> list:
        for arquivo in arquivos:
            carga_lancada = int(arquivo.get('cargaLancada'))
            carga_baixada = int(arquivo.get('cargaBaixada'))
            arquivo['pendencia'] = carga_lancada - carga_baixada
            arquivo['cargaLancada'] = int(arquivo.get('cargaLancada'))
            arquivo['cargaBaixada'] = int(arquivo.get('cargaBaixada'))

        return arquivos


    def _buscar_unidades_pendentes(self) ->None:
        dict_loec_ldi = {}
        dict_temp = DICIONARIO_PESQUISA_UNIDADES.copy()
        tipos_listas = ['OEC', 'LDI']
        dict_temp['Tipo'] = None
        lista1 = []
        for tipo in tipos_listas:
            dict_temp['Tipo_Lista'] = f'{tipo}'
            list_temp = self.sro_monitor.retorne_sro_monitor(URL_BUSCA_SRO_MONITOR_REGIONAL_UNIDADES, dict_temp)
            for unidade in list_temp:
                unidade['tipo_lista'] = tipo
            lista1.append(list_temp)
        l_teste = []
        for x in lista1[0]:
            l_teste.append(x)
        for y in lista1[1]:
            l_teste.append(y)
        self.lista_unidades_pendencias = l_teste


    def criar_unidades(self)->None:
        fabrica_unidade = FabricaUnidades()
        for unidade in self.lista_unidades_pendencias:
            nome = unidade.get('nome')
            unidade_tmp = fabrica_unidade.monte_unidades(nome)
            self.lista_unidade_criadas.append(unidade_tmp)

    def montar_unidade(self, arquivos: list) -> list:
        fabrica_unidade = FabricaUnidades()
        for arquivo in arquivos:
            nome = arquivo.get('nomeUnidade')
            unidade_tmp = fabrica_unidade.monte_unidades(nome)
            arquivo["gerencia"] = unidade_tmp.gerencia
            arquivo["vinculacao"] = unidade_tmp.vinculacao
            self.lista_unidade_ofensoras.append(arquivo)

        return self.lista_unidade_ofensoras

    def retorne_resultado_objetos(self):

        for unidade_temp in tqdm(self.lista_unidade_criadas, desc="Baixando Objetos..."):
            lista_objs_loec = self.retornar_resultato_tipos("OEC", unidade_temp.sro)
            lista_objs_ldi = self.retornar_resultato_tipos("LDI", unidade_temp.sro)
            if not lista_objs_loec == '':
                unidade_temp.salve_lista_sromonitor_oec(lista_objs_loec)

            if not lista_objs_ldi == '':
                unidade_temp.salve_lista_sromonitor_ldi(lista_objs_ldi)
            self.lista_final.append(unidade_temp)
        pass

    def retornar_resultato_tipos(self, tipo_lista, sro):
        dic_obj_temp = DICIONARIO_PESQUISA_OBJETOS
        dic_obj_temp['Codigo_SRO'] = f'{sro}'
        dic_obj_temp['Tipo_Lista'] = f'{tipo_lista}'

        unidade_com_obj = self.sro_monitor.retorne_sro_monitor(URL_BUSCA_SRO_MONITOR_REGIONAL_ONJETOS, dic_obj_temp)
        return unidade_com_obj

    def arquivar_pendencia_objetos(self) -> None:
        self.conexao.deletar_dados_tabela_bd('pendencia_loec')
        self.conexao.deletar_dados_tabela_bd('pendencia_ldi')
        for unidade_temp in self.lista_final:
            self.conexao.arquivar_dados_objetos(unidade_temp.objetos_loec, 'pendencia_loec')
            self.conexao.arquivar_dados_objetos(unidade_temp.objetos_ldi, 'pendencia_ldi')

        print('Dados de pendencias regionais salvos com sucesso!')













