import ssl
import requests
#--------------------------------------------------------------------------------------------------

#Fetch IBGE Localidades API

class TLSAdapter(requests.adapters.HTTPAdapter):
    def init_poolmanager(self, *args, **kwargs):
        ctx = ssl.create_default_context()
        ctx.set_ciphers("DEFAULT@SECLEVEL=1")
        ctx.options |= 0x4   # <-- the key part here, OP_LEGACY_SERVER_CONNECT
        kwargs["ssl_context"] = ctx
        return super(TLSAdapter, self).init_poolmanager(*args, **kwargs)


url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"


with requests.session() as s:
    s.mount("https://", TLSAdapter())
    localidades = s.get(url).json()

#--------------------------------------------------------------------------------------------------
#Armazena todos os elementos de interesse em um array de dictionaries
regioes = []
estados = []
municipios = []

for index in range(len(localidades)):

    regiao_id = localidades[index].get('microrregiao').get('mesorregiao').get('UF').get('regiao').get('id')
    regiao_sigla = localidades[index].get('microrregiao').get('mesorregiao').get('UF').get('regiao').get('sigla')
    regiao_nome = localidades[index].get('microrregiao').get('mesorregiao').get('UF').get('regiao').get('nome')
    
    if not any(regiao['id'] == regiao_id for regiao in regioes):
        regioes.append({'id': regiao_id, 'sigla': regiao_sigla, 'nome': regiao_nome})
    

    estado_id = localidades[index].get('microrregiao').get('mesorregiao').get('UF').get('id')
    estado_sigla = localidades[index].get('microrregiao').get('mesorregiao').get('UF').get('sigla')
    estado_nome = localidades[index].get('microrregiao').get('mesorregiao').get('UF').get('nome')
    estado_regiao = localidades[index].get('microrregiao').get('mesorregiao').get('UF').get('regiao').get('id')
    
    if not any(estado['id'] == estado_id for estado in estados):
        estados.append({'id': estado_id, 'sigla': estado_sigla, 'nome': estado_nome, 'regiao': estado_regiao})


    municipio_id = localidades[index].get('id')
    municipio_nome = localidades[index].get('nome')
    municipio_uf = localidades[index].get('microrregiao').get('mesorregiao').get('UF').get('id')
    if not any(municipio['id'] == municipio_id for municipio in municipios):
        municipios.append({'id': municipio_id, 'nome': municipio_nome, 'uf': municipio_uf})

#--------------------------------------------------------------------------------------------------
# Gera DUMP para um banco MySQL

with open('ibge-dump.txt', 'w') as f:
    f.write("CREATE TABLE `regiao` (`id` integer PRIMARY KEY,`sigla` varchar(255),`nome` varchar(255));\n")
    f.write("CREATE TABLE `estado` (`id` integer PRIMARY KEY,`nome` varchar(255),`sigla` varchar(255),`regiao_id` integer);\n")
    f.write("CREATE TABLE `municipio` (`id` integer PRIMARY KEY,`nome` varchar(255),`estado_id` integer);\n")
    f.write("ALTER TABLE `estado` ADD FOREIGN KEY (`regiao_id`) REFERENCES `regiao` (`id`);\n")
    f.write("ALTER TABLE `municipio` ADD FOREIGN KEY (`estado_id`) REFERENCES `estado` (`id`);\n\n")

    for index in range(len(regioes)):
        f.write(f"INSERT INTO regiao (id, sigla, nome) VALUES ({regioes[index].get('id')}, \"{regioes[index].get('sigla')}\" , \"{regioes[index].get('nome')}\");\n")
    
    f.write('\n\n')
    for index in range(len(estados)):
        f.write(f"INSERT INTO estado (id, nome, sigla, regiao_id) VALUES ({estados[index].get('id')}, \"{estados[index].get('nome')}\" , \"{estados[index].get('sigla')}\", {estados[index].get('regiao')});\n")
    
    f.write('\n\n')
    for index in range(len(municipios)):
        f.write(f"INSERT INTO municipio (id, nome, estado_id) VALUES ({municipios[index].get('id')}, \"{municipios[index].get('nome')}\", {municipios[index].get('uf')});\n")