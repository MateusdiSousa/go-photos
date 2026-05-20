#!/bin/bash

# 1. Esperar o Garage subir (ajuste o tempo se necessário)
echo "Aguardando o Garage iniciar..."
sleep 2

# 2. Capturar o ID do nó de forma limpa
NODE_ID=$(docker exec garage /garage node id -q | cut -d'@' -f1)

if [ -z "$NODE_ID" ]; then
    echo "Erro: Não foi possível obter o ID do nó. O container está rodando?"
    exit 1
fi

echo "Configurando o nó: $NODE_ID"

# 3. Associa o nó a uma zona e capacidade
docker exec garage /garage layout assign -z dc1 -c 1G $NODE_ID

# 4. APLICAR O LAYOUT (Passo crucial que faltava)
# Sem isso, qualquer comando de bucket retornará erro 500
echo "Aplicando layout..."
docker exec garage /garage layout apply --version 1

# 5. Criar a chave (Opcional: pule se já tiver a chave no .env)
# Se você já tem a 'mykey' configurada, o comando abaixo garante que ela exista
echo "Verificando chaves..."
# docker exec garage garage key create mykey 

# 6. Criar o bucket
echo "Criando bucket 'photos'..."
docker exec garage /garage bucket create photos

# 7. Criando chave para bucket de photos
docker exec garage /garage key create photos-key

# 7. Dar permissão
echo "Configurando permissões..."
docker exec garage /garage bucket allow photos --read --write --key photos-key

echo "Sucesso! Garage pronto para uso."
