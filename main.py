import psycopg2
import redis
import json

class PostgresManager:
    def __init__(self):
        self.conn = psycopg2.connect(
            host="localhost",
            database="trabalho_final",
            user="postgres",
            password="postgrestrabalho",
            port="5433"
        )
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self):
        """Cria a tabela se não existir"""
        sql = """
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            nome VARCHAR(100),
            email VARCHAR(100)
        );
        """
        self.cursor.execute(sql)
        self.conn.commit()

    def inserir(self, nome, email):
        sql = "INSERT INTO usuarios (nome, email) VALUES (%s, %s) RETURNING id;"
        self.cursor.execute(sql, (nome, email))
        user_id = self.cursor.fetchone()[0]
        self.conn.commit()
        print(f"[Postgres] Usuário inserido com ID: {user_id}")
        return user_id

    def consultar(self, user_id):
        sql = "SELECT * FROM usuarios WHERE id = %s;"
        self.cursor.execute(sql, (user_id,))
        resultado = self.cursor.fetchone()
        print(f"[Postgres] Usuário encontrado: {resultado}")
        return resultado

    def atualizar(self, user_id, novo_email):
        sql = "UPDATE usuarios SET email = %s WHERE id = %s;"
        self.cursor.execute(sql, (novo_email, user_id))
        self.conn.commit()
        print(f"[Postgres] Usuário {user_id} atualizado.")

    def deletar(self, user_id):
        sql = "DELETE FROM usuarios WHERE id = %s;"
        self.cursor.execute(sql, (user_id,))
        self.conn.commit()
        print(f"[Postgres] Usuário {user_id} removido.")

    def fechar(self):
        self.cursor.close()
        self.conn.close()


class RedisManager:
    def __init__(self):
        self.client = redis.Redis(host='localhost', port=6379, decode_responses=True)

    def inserir(self, chave, valor):
        self.client.set(chave, valor)
        print(f"[Redis] Chave '{chave}' salva.")

    def consultar(self, chave):
        valor = self.client.get(chave)
        print(f"[Redis] Valor recuperado para '{chave}': {valor}")
        return valor

    def atualizar(self, chave, novo_valor):
        if self.client.exists(chave):
            self.client.set(chave, novo_valor)
            print(f"[Redis] Chave '{chave}' atualizada.")
        else:
            print(f"[Redis] Erro: Chave '{chave}' não existe para atualizar.")

    def deletar(self, chave):
        self.client.delete(chave)
        print(f"[Redis] Chave '{chave}' removida.")


if __name__ == "__main__":
    try:
        pg = PostgresManager()
        rd = RedisManager()
        
        print("--- INICIANDO TESTES CRUD ---")

        id_user = pg.inserir("Alice Dal Paz", "alice@teste.com")
        
        dados_cache = json.dumps({"id": id_user, "nome": "Alice Dal Paz", "status": "ativo"})
        rd.inserir(f"user:{id_user}", dados_cache)

        print("-" * 30)

        pg.consultar(id_user)
        rd.consultar(f"user:{id_user}")

        print("-" * 30)

        pg.atualizar(id_user, "alice.nova@teste.com")
        
        dados_cache_novos = json.dumps({"id": id_user, "nome": "Alice Botton Dal Paz", "status": "atualizado"})
        rd.atualizar(f"user:{id_user}", dados_cache_novos)

        print("-" * 30)

        pg.deletar(id_user)
        rd.deletar(f"user:{id_user}")

        print("--- TESTES FINALIZADOS ---")
        
        pg.fechar()

    except Exception as e:
        print(f"Ocorreu um erro: {e}")
        print("Dica: Verifique se o Docker ou os serviços de banco estão rodando.")