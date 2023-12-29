from src.controller import FabricaUnidades, GerenciadorBancoDados
from src.model import ConexaoMySql, SroMonitor
from src.constantes import *
from tqdm import tqdm
import schedule
import time

def iniciar_pesquisa_pendencia_sro():

    teste_conexao = ConexaoMySql('SRO_RS')
    dados_nacionais = GerenciadorBancoDados('SRO_RS')

schedule.every(20).minutes.at(":20").do(iniciar_pesquisa_pendencia_sro)

iniciar_pesquisa_pendencia_sro()

while True:
    try:
        schedule.run_pending()
        print('Esperando para atualiazar')
        time.sleep(10)
    except:
        continue
print('atualisando...')

# teste_conexao = ConexaoMySql('SRO_RS')
# dados_nacionais = GerenciadorBancoDados('SRO_RS')
# #dados_regionais = GerenciadorBancoDados('SRO_RS')

