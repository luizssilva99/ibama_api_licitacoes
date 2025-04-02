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
        :param url_base: URL base da API para consultar os dados de PGC (padrão é a URL do Governo).
        :param cabecalhos: Cabeçalhos HTTP personalizados para a requisição.
        """
        self.url_base = url_base
        self.cabecalhos = cabecalhos or {"accept": "*/*"}
        self.todos_dados: List[Dict] = []

    def buscar_dados(self, cnpj_orgao: str, pagina: int = 1, tamanho_pagina: int = 100) -> Optional[List[Dict]]:
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
                resposta.raise_for_status()  # Lança exceção se resposta não for 2xx
                logger.info(f"Dados para o órgão {cnpj_orgao} - Página {pagina} carregados com sucesso.")
                return resposta.json().get("resultado", [])
            except requests.exceptions.RequestException as e:
                tentativas += 1
                logger.warning(f"Tentativa {tentativas} para carregar os dados do órgão {cnpj_orgao}, página {pagina} falhou. Erro: {e}")
        logger.error(f"Não foi possível carregar os dados do órgão {cnpj_orgao} após 5 tentativas.")
        return None

    def obter_dados_de_orgaos(self, cnpjs: List[str]) -> None:
        """
        Coleta dados para múltiplos órgãos.
        :param cnpjs: Lista de CNPJs dos órgãos para os quais os dados serão coletados.
        """
        for cnpj_orgao in cnpjs:
            pagina = 1
            while True:
                dados = self.buscar_dados(cnpj_orgao, pagina)
                if not dados:
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

def main():
    # Configurações que podem ser alteradas conforme necessário
    caminho_arquivo_uasg = "BASES\dados_uasg_DF.csv"  # Caminho do arquivo CSV de UASG
    caminho_salvamento_csv = "BASES\dados_pgc_FULL_DF.csv"  # Caminho onde o arquivo CSV será salvo
    
    # Lê o arquivo CSV contendo os dados de UASG
    dados_uasg_df = pd.read_csv(caminho_arquivo_uasg, sep=',', encoding='utf-8')
    cnpjs = dados_uasg_df['cnpjCpfOrgao'].dropna().unique()  # Obtém todos os CNPJs (removendo valores nulos)

    # Verifica se o diretório de salvamento existe, senão cria
    if not os.path.exists(os.path.dirname(caminho_salvamento_csv)):
        os.makedirs(os.path.dirname(caminho_salvamento_csv))
        logger.info(f"Diretório criado: {os.path.dirname(caminho_salvamento_csv)}")

    # Inicializa o coletor de dados
    coletor = ColetorDadosPgc()
    logger.info("Iniciando a coleta de dados para todos os órgãos.")
    
    # Coleta os dados
    coletor.obter_dados_de_orgaos(cnpjs)
    df = coletor.para_dataframe()
    logger.info("Dados carregados e DataFrame criado com sucesso.")
    
    # Exibe uma amostra dos dados coletados (opcional)
    logger.info(f"Exibindo os dados coletados:\n{df.head()}")

    # Salva o DataFrame em um arquivo CSV
    df.to_csv(caminho_salvamento_csv, index=False, sep=',', encoding='utf-8')
    logger.info(f"Arquivo CSV '{caminho_salvamento_csv}' salvo com sucesso.")

if __name__ == "__main__":
    main()
