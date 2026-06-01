NESSE MANUAL ESTÁ O PASSO A PASSO DE COMO SUBIR O GARAGE E COMO FAZER AS MIGRAÇÕES

1º Adicione o seguinte alias no seu computador para facilitar o trabalho:
´´´sh
alias garage="docker exec -ti <container name> /garage"
´´´

2º Execute o seguinte comando para visualizar o ID do nó do seu cluester:
```sh
garage status
```

A saida deverá ser algo como isso:
```output
==== HEALTHY NODES ====
ID                Hostname  Address         Tags  Zone  Capacity          DataAvail  Version
563e1ac825ee3323  linuxbox  127.0.0.1:3901              NO ROLE ASSIGNED             v2.3.0
```

3º Após copiar o ID do seu cluster você deverá criar um layout para seu cluster, utilizando as flags *-c* para definir a capacidade máxima do seu cluster e a *-z* para definir a localidade:
(node_id = ID adquirido na etapa passada.)
```sh
garage layout assign -z dc1 -c 10G <node_id>
```

4º Após isso você deverá aplicar o layout ao seu cluster com o seguinte comando:
```sh
garage layout apply --version 1
```

5º Após finalizar essa etapa, abra o terminal nessa mesma pasta, e execute os seguinte comandos:
```sh
go mod tidy
go run migrator.go
```
O script irá criar todos os buckets e chaves necessárias para o garage e após isso você terá um arquivo .env com os dados da SECRET ID E KEY ID, o nomes seguiram o seguinte formato SECRET_KEY_NOME_CHAVE, e ID_KEY_NOME_CHAVE, exemplo: SECRET_KEY_PHOTOS_KEY e ID_KEY_PHOTOS_KEY

