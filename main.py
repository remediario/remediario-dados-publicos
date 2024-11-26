import pandas as pd
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://feumpi:oT9KImFdnTvT3p6j@cluster0.bvdr7.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))
# Send a ping to confirm a successful connection
try:
  client.admin.command('ping')
  print("Conectado ao banco de dados.")
except Exception as e:
  print(e)

df = pd.read_csv("DADOS_ABERTOS_MEDICAMENTOS.csv", sep=";",
                 encoding="latin1",
                 usecols=["NOME_PRODUTO", "CATEGORIA_REGULATORIA", "CLASSE_TERAPEUTICA",
                          "EMPRESA_DETENTORA_REGISTRO", "SITUACAO_REGISTRO", "PRINCIPIO_ATIVO"],
                 )

df = df.rename(columns={"NOME_PRODUTO": "nome",
                        "CATEGORIA_REGULATORIA": "categoria",
                        "CLASSE_TERAPEUTICA": "classe",
                        "EMPRESA_DETENTORA_REGISTRO": "fabricante",
                        "SITUACAO_REGISTRO": "situacao",
                        "PRINCIPIO_ATIVO": "principioAtivo"})

df = df[df["situacao"] == "VÁLIDO"]

df["fabricante"] = df["fabricante"].map(
    lambda fabricante: fabricante.split(" - ")[1])
df = df.reset_index(drop=True)
print(len(df))
print(df.head())
print(df.columns)

db = client["remediario"]
collection = db["remediosAnvisa"]
collection.delete_many({})
collection.insert_many(df.to_dict("records"))

print("Dados inseridos com sucesso.")
