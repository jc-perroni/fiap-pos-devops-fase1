#!/bin/sh

# O que este script faz:
# 1. Checa as variáveis de ambiente para o banco de dados.
# 2. Entra em um loop que tenta se conectar ao banco de dados.
# 3. Só sai do loop quando o banco de dados está pronto para aceitar conexões.
# 4. Executa o comando de inicialização do banco de dados.
# 5. Inicia o servidor Gunicorn.

# Verifique se as variáveis de ambiente do banco de dados estão definidas
if [ -z "$DB_HOST" ] || [ -z "$DB_PORT" ] || [ -z "$DB_NAME" ] || [ -z "$DB_USER" ]; then
  echo "Erro: As variáveis de ambiente do banco de dados (DB_HOST, DB_PORT, DB_NAME, DB_USER) devem ser definidas."
  exit 1
fi

# Garante que o Flask CLI consiga localizar a aplicação
export FLASK_APP=${FLASK_APP:-app}

echo "Aguardando o banco de dados em ${DB_HOST}:${DB_PORT}..."

# Loop para aguardar o banco de dados ficar disponível
# Para PostgreSQL, podemos usar o `pg_isready`
# Adicione `postgresql-client` ao seu Dockerfile (ex: apt-get install -y postgresql-client)
while ! pg_isready -h "$DB_HOST" -p "$DB_PORT" -q -U "$DB_USER"; do
  echo "Banco de dados indisponível - aguardando..."
  sleep 1
done

echo "Banco de dados disponível!"

# Executa o comando de inicialização/migração do banco de dados
echo "Executando a inicialização do banco de dados..."
flask init-db

# Inicia a aplicação principal (Gunicorn)
echo "Iniciando o servidor Gunicorn..."
exec "$@"