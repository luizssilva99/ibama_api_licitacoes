import requests
import pandas as pd
from datetime import datetime
import logging
from typing import List, Dict, Optional
import os

# Configuração do logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger()

class ColetorDadosUasg:
    def __init__(self, url_base: str = "https://dadosabertos.compras.gov.br/modulo-uasg/1_consultarUasg", 
                 cabecalhos: Optional[Dict[str, str]] = None):
        """
        Inicializa a classe ColetorDadosUasg.
        :param url_base: URL base da API para consultar os dados das UASGs.
        :param cabecalhos: Cabeçalhos HTTP personalizados para a requisição.
        """
        self.url_base = url_base
        self.cabecalhos = cabecalhos or {"accept": "*/*"}
        self.todos_dados: List[Dict] = []

    def buscar_dados(self, pagina: int) -> Optional[List[Dict]]:
        """
        Busca os dados da página especificada na URL.
        :param pagina: Número da página de dados a ser recuperada.
        :return: Lista de dicionários com os dados da página ou None em caso de erro.
        """
        url = f"{self.url_base}?pagina={pagina}&usoSisg=true&statusUasg=true"
        tentativas = 0
        while tentativas < 5:
            try:
                resposta = requests.get(url, headers=self.cabecalhos)
                resposta.raise_for_status()  # Lança exceção se resposta não for 2xx
                logger.info(f"Página {pagina} carregada com sucesso.")
                return resposta.json().get("resultado", [])
            except requests.exceptions.RequestException as e:
                tentativas += 1
                logger.warning(f"Tentativa {tentativas} para carregar a página {pagina} falhou. Erro: {e}")
        logger.error(f"Não foi possível carregar a página {pagina} após 5 tentativas.")
        return None

    def obter_todos_dados(self) -> List[Dict]:
        """
        Coleta todos os dados, fazendo a paginação automaticamente.
        :return: Lista com todos os dados coletados.
        """
        pagina = 1
        while True:
            dados = self.buscar_dados(pagina)
            if not dados:
                break
            self.todos_dados.extend(dados)
            logger.info(f"Página {pagina} processada com sucesso.")
            pagina += 1
        return self.todos_dados

    def para_dataframe(self) -> pd.DataFrame:
        """
        Converte os dados coletados para um DataFrame do pandas.
        :return: DataFrame com os dados coletados.
        """
        logger.info("Convertendo dados para DataFrame.")
        return pd.DataFrame(self.todos_dados)

def corrigir_cnpj(df: pd.DataFrame) -> pd.DataFrame:
    """
    Verifica a coluna 'cnpjCpfOrgao' e ajusta os valores:A
    - Transforma o valor para string.
    - Se o valor for composto apenas por dígitos e tiver 13 caracteres (ou seja, faltando o zero à esquerda), 
      adiciona um "0" à esquerda.
    """
    if 'cnpjCpfOrgao' in df.columns:
        def ajustar_valor(valor):
            valor_str = str(valor).strip()
            # Se for apenas dígitos e tiver 13 caracteres, adiciona o zero à esquerda.
            if valor_str.isdigit() and len(valor_str) == 13:
                return "0" + valor_str      
            return valor_str

        df['cnpjCpfOrgao'] = df['cnpjCpfOrgao'].apply(ajustar_valor)
        logger.info("Coluna 'cnpjCpfOrgao' ajustada com sucesso.")
    else:
        logger.warning("Coluna 'cnpjCpfOrgao' não encontrada no DataFrame.")
    return df

def main():
    # Configurações que podem ser alteradas conforme necessário
    url_base = "https://dadosabertos.compras.gov.br/modulo-uasg/1_consultarUasg"  # URL da API
    caminho_salvamento_csv_dirty = "BASES/dados_uasg_DIRTY.csv"  # Caminho onde o arquivo dirty será salvo
    caminho_salvamento_csv_df = "BASES/dados_uasg_DF.csv"  # Caminho onde o arquivo filtrado será salvo

    # Garante que o diretório de salvamento exista
    if not os.path.exists(os.path.dirname(caminho_salvamento_csv_dirty)):
        os.makedirs(os.path.dirname(caminho_salvamento_csv_dirty))
        logger.info(f"Diretório criado: {os.path.dirname(caminho_salvamento_csv_dirty)}")

    # Inicializa o coletor de dados
    coletor = ColetorDadosUasg(url_base=url_base)
    logger.info("Iniciando a coleta de dados.")
    
    # Coleta os dados e converte para DataFrame
    coletor.obter_todos_dados()
    df = coletor.para_dataframe()
    logger.info("Dados carregados e DataFrame criado com sucesso.")
    
    # Exibe uma amostra dos dados coletados (opcional)
    logger.info(f"Exibindo os dados coletados:\n{df.head()}")

    # Corrige a coluna 'cnpjCpfOrgao'
    df = corrigir_cnpj(df)
    
    # Salva o DataFrame completo (dirty) em um arquivo CSV 
    df.to_csv(caminho_salvamento_csv_dirty, index=False)
    logger.info(f"Arquivo CSV '{caminho_salvamento_csv_dirty}' salvo com sucesso.")

    # Filtra os dados onde 'siglaUf' seja igual a 'DF'
    df_df = df[df['siglaUf'] == 'DF']
    df_df.to_csv(caminho_salvamento_csv_df, index=False)
    logger.info(f"Arquivo CSV filtrado '{caminho_salvamento_csv_df}' (apenas 'DF') salvo com sucesso.")

if __name__ == "__main__":
    main()
