import requests
import pandas as pd
from datetime import datetime

class ColetorDadosOrgao:
    def __init__(self):
        self.url_base = "https://dadosabertos.compras.gov.br/modulo-uasg/2_consultarOrgao"
        self.cabecalhos = {"accept": "*/*"}
        self.todos_dados = []

    def buscar_dados(self, pagina):
        url = f"{self.url_base}?pagina={pagina}&statusOrgao=true"
        tentativas = 0
        while tentativas < 5:
            resposta = requests.get(url, headers=self.cabecalhos)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if resposta.status_code == 200:
                print(f"[{timestamp}] Página {pagina} carregada com sucesso.")
                return resposta.json().get("resultado", [])
            tentativas += 1
            print(f"[{timestamp}] Tentativa {tentativas} para carregar a página {pagina} falhou.")
        print(f"[{timestamp}] Não foi possível carregar a página {pagina} após 5 tentativas.")
        return None

    def obter_todos_dados(self):
        pagina = 1
        while True:
            dados = self.buscar_dados(pagina)
            if not dados:
                break
            self.todos_dados.extend(dados)
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] Página {pagina} processada com sucesso.")
            pagina += 1
        return self.todos_dados

    def para_dataframe(self):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] Convertendo dados para DataFrame.")
        return pd.DataFrame(self.todos_dados)

# Instanciando a classe e usando-a para pegar os dados e convertê-los em um DataFrame
timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
print(f"[{timestamp}] Iniciando a coleta de dados.")
coletor = ColetorDadosOrgao()
coletor.obter_todos_dados()
df = coletor.para_dataframe()
print(f"[{timestamp}] Dados carregados e DataFrame criado com sucesso.")

# Mostrar a tabela final
print(df)

# Opcional: salvar a tabela em um arquivo CSV
df.to_csv("BASES/dados_orgaos_DIRTY.csv", index=False)