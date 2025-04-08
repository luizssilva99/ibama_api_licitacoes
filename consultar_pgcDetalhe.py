import requests
import pandas as pd
from datetime import datetime
import logging
from typing import List, Dict, Optional
import os

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()


class ColetorDadosPgc:
    def __init__(self, url_base: str = "https://dadosabertos.compras.gov.br/modulo-pgc/1_consultarPgcDetalhe",
                 cabecalhos: Optional[Dict[str, str]] = None):
        """
        Inicializa a classe ColetorDadosPgc.
        :param url_base: URL base da API para consultar os dados de PGC.
        :param cabecalhos: Cabeçalhos HTTP personalizados para a requisição.
        """
        self.url_base = url_base
        self.cabecalhos = cabecalhos or {"accept": "*/*"}
        self.todos_dados: List[Dict] = []

    def buscar_dados(self, cnpj_orgao: str, pagina: int = 1, tamanho_pagina: int = 10) -> Optional[List[Dict]]:
        """
        Busca os dados de um órgão para uma página específica.
        :param cnpj_orgao: CNPJ do órgão para buscar os dados.
        :param pagina: Número da página a ser carregada.
        :param tamanho_pagina: Número de itens por página.
        :return: Lista de dicionários com os dados da página ou None se falhar.
        """
        url = f"{self.url_base}?pagina={pagina}&tamanhoPagina={tamanho_pagina}&orgao={cnpj_orgao}&anoPcaProjetoCompra=2025"
        tentativas = 0
        while tentativas < 5:
            try:
                resposta = requests.get(url, headers=self.cabecalhos)
                resposta.raise_for_status()  # Lança exceção se a resposta não for 2xx
                logger.info(f"Dados para o órgão {cnpj_orgao} - Página {pagina} carregados com sucesso.")
                return resposta.json().get("resultado", [])
            except requests.exceptions.RequestException as e:
                tentativas += 1
                logger.warning(f"Tentativa {tentativas} para carregar dados do órgão {cnpj_orgao}, página {pagina} falhou. Erro: {e}")
        logger.error(f"Não foi possível carregar dados do órgão {cnpj_orgao} após 5 tentativas.")
        return None

    def obter_dados_de_orgaos(self, cnpjs: List[str]) -> None:
        """
        Coleta dados para múltiplos órgãos.
        :param cnpjs: Lista de CNPJs dos órgãos para os quais os dados serão coletados.
        """
        total = len(cnpjs)
        for i, cnpj_orgao in enumerate(cnpjs, 1):
            logger.info(f"Processando {i}/{total}: {cnpj_orgao}")
            pagina = 1
            while True:
                dados = self.buscar_dados(cnpj_orgao, pagina)
                # Se a API retornar None ou uma lista vazia, não há mais dados para esse CNPJ
                if dados is None or len(dados) == 0:
                    logger.info(f"Sem mais dados para o órgão {cnpj_orgao} na página {pagina}.")
                    break
                self.todos_dados.extend(dados)
                logger.info(f"Dados do órgão {cnpj_orgao} - Página {pagina} processados com sucesso.")
                pagina += 1

    def para_dataframe(self) -> pd.DataFrame:
        """
        Converte os dados coletados para um DataFrame do pandas.
        :return: DataFrame com os dados coletados.
        """
        logger.info("Convertendo dados para DataFrame.")
        return pd.DataFrame(self.todos_dados)


def ajustar_cnpj(cnpj, tamanho=14):
    """
    Verifica e ajusta o CNPJ para garantir que seja uma string com o tamanho especificado.
    Caso o CNPJ não atinja o comprimento esperado, adiciona zeros à esquerda.
    :param cnpj: O CNPJ original.
    :param tamanho: Tamanho esperado do CNPJ (padrão 14).
    :return: CNPJ ajustado como string.
    """
    cnpj_str = str(cnpj)
    if len(cnpj_str) < tamanho:
        cnpj_str = cnpj_str.zfill(tamanho)
    return cnpj_str


def main():
    # Configurações
    caminho_arquivo_uasg = r"BASES\dados_uasg_FILTRADO.csv"  # Arquivo de entrada
    caminho_salvamento_csv = r"BASES\dados_pgc_FULL_DF_2025.csv"  # Arquivo de saída
    
    # Lê o arquivo CSV contendo os dados de UASG
    dados_uasg_df = pd.read_csv(caminho_arquivo_uasg, sep=',', encoding='utf-8')
    logger.info(f"Colunas do CSV: {dados_uasg_df.columns.tolist()}")

    # Verifica qual coluna de CNPJ está disponível
    if 'cnpj_orgao' in dados_uasg_df.columns:
        coluna_cnpj = 'cnpj_orgao'
    elif 'cnpjCpfOrgao' in dados_uasg_df.columns:
        coluna_cnpj = 'cnpjCpfOrgao'
    else:
        logger.error("Nenhuma coluna de CNPJ encontrada no CSV.")
        return

    # Extrai TODOS os CNPJs (caso queira evitar duplicatas, remova o .tolist() e substitua por .unique())
    cnpjs = dados_uasg_df[coluna_cnpj].dropna().tolist()
    
    # Ajusta cada CNPJ para que seja uma string de 14 dígitos (preenchendo com zeros à esquerda se necessário)
    cnpjs = [ajustar_cnpj(cnpj) for cnpj in cnpjs]
    logger.info(f"Total de CNPJs a processar: {len(cnpjs)}")
    
    # Verifica se o diretório de salvamento existe; caso contrário, cria o diretório
    if not os.path.exists(os.path.dirname(caminho_salvamento_csv)):
        os.makedirs(os.path.dirname(caminho_salvamento_csv))
        logger.info(f"Diretório criado: {os.path.dirname(caminho_salvamento_csv)}")

    # Inicializa o coletor de dados e realiza a coleta para cada CNPJ
    coletor = ColetorDadosPgc()
    logger.info("Iniciando a coleta de dados para todos os órgãos.")
    
    coletor.obter_dados_de_orgaos(cnpjs)
    df = coletor.para_dataframe()
    logger.info("Dados carregados e DataFrame criado com sucesso.")
    logger.info(f"Exibindo amostra dos dados coletados:\n{df.head()}")

    # Salva o DataFrame em um arquivo CSV
    df.to_csv(caminho_salvamento_csv, index=False, sep=',', encoding='utf-8')
    logger.info(f"Arquivo CSV '{caminho_salvamento_csv}' salvo com sucesso.")


if __name__ == "__main__":
    main()
import requests
import pandas as pd
from datetime import datetime
import logging
from typing import List, Dict, Optional
import os

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()


class ColetorDadosPgc:
    def __init__(self, url_base: str = "https://dadosabertos.compras.gov.br/modulo-pgc/1_consultarPgcDetalhe",
                 cabecalhos: Optional[Dict[str, str]] = None):
        """
        Inicializa a classe ColetorDadosPgc.
        :param url_base: URL base da API para consultar os dados de PGC.
        :param cabecalhos: Cabeçalhos HTTP personalizados para a requisição.
        """
        self.url_base = url_base
        self.cabecalhos = cabecalhos or {"accept": "*/*"}
        self.todos_dados: List[Dict] = []

    def buscar_dados(self, cnpj_orgao: str, pagina: int = 1, tamanho_pagina: int = 10) -> Optional[List[Dict]]:
        """
        Busca os dados de um órgão para uma página específica.
        :param cnpj_orgao: CNPJ do órgão para buscar os dados.
        :param pagina: Número da página a ser carregada.
        :param tamanho_pagina: Número de itens por página.
        :return: Lista de dicionários com os dados da página ou None se falhar.
        """
        url = f"{self.url_base}?pagina={pagina}&tamanhoPagina={tamanho_pagina}&orgao={cnpj_orgao}&anoPcaProjetoCompra=2025"
        tentativas = 0
        while tentativas < 5:
            try:
                resposta = requests.get(url, headers=self.cabecalhos)
                resposta.raise_for_status()  # Lança exceção se a resposta não for 2xx
                logger.info(f"Dados para o órgão {cnpj_orgao} - Página {pagina} carregados com sucesso.")
                return resposta.json().get("resultado", [])
            except requests.exceptions.RequestException as e:
                tentativas += 1
                logger.warning(f"Tentativa {tentativas} para carregar dados do órgão {cnpj_orgao}, página {pagina} falhou. Erro: {e}")
        logger.error(f"Não foi possível carregar dados do órgão {cnpj_orgao} após 5 tentativas.")
        return None

    def obter_dados_de_orgaos(self, cnpjs: List[str]) -> None:
        """
        Coleta dados para múltiplos órgãos.
        :param cnpjs: Lista de CNPJs dos órgãos para os quais os dados serão coletados.
        """
        total = len(cnpjs)
        for i, cnpj_orgao in enumerate(cnpjs, 1):
            logger.info(f"Processando {i}/{total}: {cnpj_orgao}")
            pagina = 1
            while True:
                dados = self.buscar_dados(cnpj_orgao, pagina)
                # Se a API retornar None ou uma lista vazia, não há mais dados para esse CNPJ
                if dados is None or len(dados) == 0:
                    logger.info(f"Sem mais dados para o órgão {cnpj_orgao} na página {pagina}.")
                    break
                self.todos_dados.extend(dados)
                logger.info(f"Dados do órgão {cnpj_orgao} - Página {pagina} processados com sucesso.")
                pagina += 1

    def para_dataframe(self) -> pd.DataFrame:
        """
        Converte os dados coletados para um DataFrame do pandas.
        :return: DataFrame com os dados coletados.
        """
        logger.info("Convertendo dados para DataFrame.")
        return pd.DataFrame(self.todos_dados)


def ajustar_cnpj(cnpj, tamanho=14):
    """
    Verifica e ajusta o CNPJ para garantir que seja uma string com o tamanho especificado.
    Caso o CNPJ não atinja o comprimento esperado, adiciona zeros à esquerda.
    :param cnpj: O CNPJ original.
    :param tamanho: Tamanho esperado do CNPJ (padrão 14).
    :return: CNPJ ajustado como string.
    """
    cnpj_str = str(cnpj)
    if len(cnpj_str) < tamanho:
        cnpj_str = cnpj_str.zfill(tamanho)
    return cnpj_str


def main():
    # Configurações
    caminho_arquivo_uasg = r"BASES\dados_uasg_FILTRADO.csv"  # Arquivo de entrada
    caminho_salvamento_csv = r"BASES\dados_pgc_FULL_DF_2025.csv"  # Arquivo de saída
    
    # Lê o arquivo CSV contendo os dados de UASG
    dados_uasg_df = pd.read_csv(caminho_arquivo_uasg, sep=',', encoding='utf-8')
    logger.info(f"Colunas do CSV: {dados_uasg_df.columns.tolist()}")

    # Verifica qual coluna de CNPJ está disponível
    if 'cnpj_orgao' in dados_uasg_df.columns:
        coluna_cnpj = 'cnpj_orgao'
    elif 'cnpjCpfOrgao' in dados_uasg_df.columns:
        coluna_cnpj = 'cnpjCpfOrgao'
    else:
        logger.error("Nenhuma coluna de CNPJ encontrada no CSV.")
        return

    # Extrai TODOS os CNPJs (caso queira evitar duplicatas, remova o .tolist() e substitua por .unique())
    cnpjs = dados_uasg_df[coluna_cnpj].dropna().tolist()
    
    # Ajusta cada CNPJ para que seja uma string de 14 dígitos (preenchendo com zeros à esquerda se necessário)
    cnpjs = [ajustar_cnpj(cnpj) for cnpj in cnpjs]
    logger.info(f"Total de CNPJs a processar: {len(cnpjs)}")
    
    # Verifica se o diretório de salvamento existe; caso contrário, cria o diretório
    if not os.path.exists(os.path.dirname(caminho_salvamento_csv)):
        os.makedirs(os.path.dirname(caminho_salvamento_csv))
        logger.info(f"Diretório criado: {os.path.dirname(caminho_salvamento_csv)}")

    # Inicializa o coletor de dados e realiza a coleta para cada CNPJ
    coletor = ColetorDadosPgc()
    logger.info("Iniciando a coleta de dados para todos os órgãos.")
    
    coletor.obter_dados_de_orgaos(cnpjs)
    df = coletor.para_dataframe()
    logger.info("Dados carregados e DataFrame criado com sucesso.")
    logger.info(f"Exibindo amostra dos dados coletados:\n{df.head()}")

    # Salva o DataFrame em um arquivo CSV
    df.to_csv(caminho_salvamento_csv, index=False, sep=',', encoding='utf-8')
    logger.info(f"Arquivo CSV '{caminho_salvamento_csv}' salvo com sucesso.")


if __name__ == "__main__":
    main()
