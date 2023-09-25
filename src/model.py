from datetime import timedelta, date
from boltons.iterutils import remap
import pymysql.cursors
from tqdm import tqdm
import datetime as dt
import requests
import holidays
import calendar
import locale
import re


class Calendario:

    def __init__(self):
        self.dia_atual_datatime = None
        self.dia_ontem_datatime = None
        self.dia_ontem_barras = None
        self.dia_ontem_hifen = None
        self.dia_atual_barras = None
        self.dia_atual_hifen = None
        self.mes_atual_numero = None
        self.mes_atual_nome = None
        self.ano_atual = None
        self.converter_data_str()
        objeto_dia_ontem = DiaOntem(self.dia_ontem_datatime)
        self.dia_ontem_relatorio_regional = objeto_dia_ontem.data_texto
        objeto_dia_ontem = DiaOntenNacional(self.dia_atual_datatime)
        self.dia_ontem_relatorio_nacional = objeto_dia_ontem.data_texto

    def converter_data_str(self):
        locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')
        data = dt.datetime.now().date()

        self.dia_atual_barras = data.strftime('%d/%m/%Y')
        self.dia_atual_hifen = data.strftime('%d-%m-%Y')

        data_ontem = data - timedelta(1)
        self.dia_ontem_barras = data_ontem.strftime('%d/%m/%Y')
        self.dia_ontem_hifen = data_ontem.strftime('%d-%m-%Y')

        mes_temp = data.strftime('%B')
        self.mes_atual_nome = mes_temp.capitalize()
        self.mes_atual_numero = data.strftime('%m')
        self.ano_atual = data.strftime('%Y')

        self.dia_atual_datatime = data
        self.dia_ontem_datatime = data_ontem



        pass


class DiaOntenNacional:
    def __init__(self, data):
        self.data_texto = None
        self.data = data
        if self._confirmar_segunda():
            self._formatar_para_segunda()
        else:
            self._formatar_para_outro_dias()

    def _confirmar_segunda(self):
        locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')
        dia_semana = calendar.day_name[self.data.weekday()]

        if dia_semana == 'segunda':
            return True
        return False

    def _formatar_para_segunda(self) -> None:
        data = self.data - timedelta(2)
        self.data_texto = data.strftime('%d/%m/%Y')

    def _formatar_para_outro_dias(self) -> None:
        data = self.data - timedelta(1)
        self.data_texto = data.strftime('%d/%m/%Y')


class DiaOntem:

    def __init__(self, data):
        self.data = data
        self.confirma_final_semana()
        self.feriados = holidays.Brazil()
        self.feriados_rs = holidays.Brazil(state='RS')
        self.verifica_feriado()
        self.data_texto = self.data.strftime('%d/%m/%Y')


    def confirma_final_semana(self):
        if self.verifica_final_semana():
            locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')
            dia_semana = calendar.day_name[self.data.weekday()]

            if dia_semana == 'sábado':
                self.data = self.data - timedelta(1)

            if dia_semana == 'domingo':
                self.data = self.data - timedelta(2)
        else:
            print('nao e final de semana')

    def verifica_final_semana(self) -> bool:
        locale.setlocale(locale.LC_ALL, 'pt_BR.utf8')
        dia_semana = calendar.day_name[self.data.weekday()]

        if dia_semana == 'sábado':
            return True

        if dia_semana == 'domingo':
            return True

        return False

    def verifica_feriado(self) -> None:
        if self.data in self.feriados or self.data in self.feriados_rs:
            self.data = self.data - timedelta(1)
            if self.verifica_final_semana():
                print('final de semana')
            else:
                print('nao e final de semana')


class Unidade:
    gerencia: str
    nome: str
    sro: str
    vinculacao: str
    email: str
    mcu: str

    def __init__(self, gerencia, nome, sro, vinculacao, email, mcu):
        self.gerencia = gerencia
        self.nome = nome
        self.sro = sro
        self.vinculacao = vinculacao
        self.email = email
        self.mcu = mcu
        self.carga_lancada = None
        self.carga_baixada = None
        self.pendencia = None
        self.percentual = None
        self.data = None
        self.tipo_unidade = None
        self.objetos_ldi = []
        self.objetos_loec = []


    def salvar_objetos(self, lista, tipo_lista):
        if tipo_lista == 'OEC':
            self.objetos_loec = lista
            return
        self.objetos_ldi = lista


    def salve_lista_sromonitor_ldi(self, lista_temp) -> None:
        for dic in lista_temp:
            dic['gerencia'] = self.gerencia
            dic['mcu'] = self.mcu
            dic['sro'] = self.sro
            dic['nomeUnidade'] = self.nome
            dic['vinculacao'] = self.vinculacao
            dic['nomeUnidade'] = self.nome
            dic['vinculacao'] = self.nome
            dic.pop('tempo', None)

            self.objetos_ldi.append(dic)


    def salve_lista_sromonitor_oec(self, lista_temp) -> None:
        for dic in lista_temp:
            dic['gerencia'] = self.gerencia
            dic['mcu'] = self.mcu
            dic['sro'] = self.sro
            dic['nomeUnidade'] = self.nome
            dic['vinculacao'] = self.vinculacao
            dic.pop('tempo', None)
            dic.pop('prazo', None)

            self.objetos_loec.append(dic)


class ConexaoMySql:
    host: str
    user: str
    password: str
    database: str

    def __init__(self, database: str) -> None:
        self.conexao_DB = pymysql.connect(
            host='localhost',
            user='root',
            password='ect-2023',
            database=f'{database}',
            cursorclass=pymysql.cursors.DictCursor
        )
        print(f'Conectado ao sql: {database}')

    def criar_tabela(self, nome_tabela: str, dados: dict) -> None:
        # Edita tabela Banco de dados

        # preparar cursor com metodo cursor
        with self.conexao_DB.cursor() as c:
            # cod_sql = self.limpar_dict(nome_tabela, dados)
            cod_sql = self.criar_tabela_lista_dicionario(nome_tabela, dados)

            c.execute(cod_sql)
            self.conexao_DB.commit()
            # self.conexao_DB.close()
            print('Tabela criada com sucesso')

    def limpar_dict(self, nome_tabela, dados: dict) -> str:

        # for k, v in dados.items():
        teste = str(dados)
        dados_temp1 = teste.replace('{', '(')
        dados_temp2 = dados_temp1.replace('}', ')')
        dados_temp = re.sub(r":|'", '', dados_temp2)
        dados_retorno = re.sub(r',(\w*)', ',\n', dados_temp)

        cod_sql = f'''CREATE TABLE if NOT EXISTS `{nome_tabela}`
                    {dados_retorno}
                    ENGINE = InnoDB 
                    DEFAULT CHARSET=utf8mb4 
                    COLLATE=utf8mb4_bin 
                    AUTO_INCREMENT=1;'''

        return cod_sql

    def criar_tabela_lista_dicionario(self, nome_tabela: str, lista_dic: list) -> str:
        for dic in tqdm(lista_dic, desc=f"Salvando dados em {nome_tabela}"):
            columns = ', '.join("`" + str(x) + "`" for x in dic.keys())
            # values = ', '.join("'" + str(x) + "'" for x in dic.values())
            sql: str = "INSERT INTO %s ( %s );" % (f'{nome_tabela}', columns)
            return sql

    def arquivar_dados(self, dados: list, nome_tabela: str) -> None:
        """
        Itera a lista de dicionarios,
        Gera nome das colunas baseado nas chaves
        Gera values baseado no valor
        """


        with self.conexao_DB.cursor() as c:
            for dado in tqdm(dados, desc=f"Salvando dados em {nome_tabela}"):
                data = dado.get('data')
                siglaSE = dado.get("siglaSE")

                if self.consulta_presenca_banco(f'{nome_tabela}', 'siglaSE', siglaSE, 'data', data) == False and nome_tabela == "ano_2023":
                    pass
                    columns = ', '.join("`" + str(x) + "`" for x in dado.keys())
                    values = ', '.join("'" + str(x) + "'" for x in dado.values())
                    sql: str = "INSERT INTO %s ( %s ) VALUES ( %s );" % (f'{nome_tabela}', columns, values)
                    c.execute(sql)
                    self.conexao_DB.commit()
                    print('Tabela Nacional atualizada com sucesso')
                elif nome_tabela == 'pendencia_ofensoras':
                    columns = ', '.join("`" + str(x) + "`" for x in dado.keys())
                    values = ', '.join("'" + str(x) + "'" for x in dado.values())
                    sql: str = "INSERT INTO %s ( %s ) VALUES ( %s );" % (f'{nome_tabela}', columns, values)
                    c.execute(sql)
                    self.conexao_DB.commit()
                    print('Tabela Regional atualizada com sucesso')

                else:
                    print('Sem atualização')

    def arquivar_dados_objetos(self, dados: list, nome_tabela: str) -> None:

        """
        Itera a lista de dicionarios,
        Gera nome das colunas baseado nas chaves
        Gera values baseado no valor
        """

        with self.conexao_DB.cursor() as c:
            for dado in tqdm(dados, desc=f"Salvando dados em {nome_tabela}"):

                    columns = ', '.join("`" + str(x) + "`" for x in dado.keys())
                    values = ', '.join("'" + str(x) + "'" for x in dado.values())
                    sql: str = "INSERT INTO %s ( %s ) VALUES ( %s );" % (f'{nome_tabela}', columns, values)
                    c.execute(sql)
                    self.conexao_DB.commit()

        # self.conexao_DB.close()

    def consulta_sql(self, cod_sql) -> list:
        lista_consulta = []
        with self.conexao_DB.cursor() as c:
            c.execute(cod_sql)
            consulta = c.fetchall()
            lista_consulta.append(consulta[0])

        return lista_consulta

    def consulta_presenca_banco(self, nome_tabela: str, coluna1: str, valor1: str, coluna2: str, valor2: str) -> bool:

        with self.conexao_DB.cursor() as c:
            # od_sql = f"SELECT *FROM {nome_tabela} WHERE Unidade = {unidade};"

            cod_sql = f"SELECT * FROM `{nome_tabela}` WHERE `{coluna1}` LIKE '{valor1}' AND `{coluna2}` LIKE '{valor2}';"

            c.execute(cod_sql)
            if c.fetchone():
                return True
            return False

    def consulta_ultimo_registro_banco(self, nome_banco: str) -> str:

        with self.conexao_DB.cursor() as c:
            cod_sql = 'SELECT data FROM `ano_2023` ORDER BY id DESC LIMIT 1'
            c.execute(cod_sql)
            consulta = c.fetchall()
            return consulta[0]['data']

    def deletar_dados_tabela_bd(self, nome_tabela: str)-> None:
        with self.conexao_DB.cursor() as c:
            cod_sql = f"TRUNCATE TABLE `{nome_tabela}`;"
            c.execute(cod_sql)

class SroMonitor:

    def retorne_sro_monitor(self, url: object, dicionario: dict) -> dict:
        x = requests.post(url, data=dicionario)
        test = x.json()

        # Limpa dicionario, apaga chaves com valor vazio
        limpa_dict = lambda path, key, value: bool(value)
        dict_limpo = remap(test, visit=limpa_dict)
        try:
            dict_pronto = dict_limpo[0]
            return dict_pronto
        except IndexError:
            return dict_limpo
