# tech-challenge-2

### Subir Docker containers
Instale o docker (docker server também ou docker desktop) e o docker-compose;

rode o comando:
- docker-compose up -d --build

Os seguintes containers serão criados:
![containers.png](containers.png)

### Database
Acesse o banco de dados:

1. HOST: localhost
2. PORT: 3306
3. USER: root
4. PASSWORD: password
5. DATABASE: tech

#### Criar a tabela
Execute o script `init_database_source_bronze.ipynb`, este irá criar a tabela no banco de dados e inserir alguns registros.

### Testando o kafka
Com os containers rodando, acesse o kafka-ui:
http://localhost:8082/

Será possível acessar a UI do kafka:
![kafka-ui.png](kafka-ui.png)

Obs: caso tenha acabado de subir o container, espere alguns segundos para que o kafka seja iniciado e atualize a lista de tópicos.

Vá no tópico `tech-challenge.events` e crie um evento (produce message):

Mensagem de exemplo para body:

```` json
 {
    "ano": 2023,
    "sigla_uf": "SP",
    "serie": 2,
    "rede": 1,
    "taxa_alfabetizacao": 85.5,
    "media_portugues": 210.4,
    "proporcao_aluno_nivel_0": 5,
    "proporcao_aluno_nivel_1": 10.2,
    "proporcao_aluno_nivel_2": 15.4,
    "proporcao_aluno_nivel_3": 20.1,
    "proporcao_aluno_nivel_4": 18.3,
    "proporcao_aluno_nivel_5": 12.5,
    "proporcao_aluno_nivel_6": 8.0,
    "proporcao_aluno_nivel_7": 6.2,
    "proporcao_aluno_nivel_8": 4.3
  }
````

Depois execute o `stream_source_bronze.ipynb` para consumir o evento.

Acesse o Banco de dados e verifique se o dado foi inserido conforme o json acima.

