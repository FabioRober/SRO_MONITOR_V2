from src.controller import FabricaUnidades, GerenciadorBancoDados
from src.model import ConexaoMySql, SroMonitor
from src.constantes import *
from tqdm import tqdm


teste_conexao = ConexaoMySql('SRO_RS')
Dados_Nacionais = GerenciadorBancoDados('SRO_RS')
dados_regionais = GerenciadorBancoDados('SRO_RS')





