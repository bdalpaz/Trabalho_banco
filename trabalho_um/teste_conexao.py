from neo4j import GraphDatabase

URI = "bolt://localhost:7687"
USUARIO = "neo4j"
SENHA = "trabalho"  

try:
    driver = GraphDatabase.driver(URI, auth=(USUARIO, SENHA))
    
    driver.verify_connectivity()
    print("Conexão bem-sucedida!")

    with driver.session(database="neo4j") as session:
        query = "MATCH (p:Pessoa) RETURN p.nome AS nome"
        result = session.run(query)

        print("Pessoas encontradas no banco:")
        count = 0
        for record in result:
            print(record["nome"])
            count += 1
        
        if count == 0:
            print("(Nenhuma pessoa encontrada)")

except Exception as e:
    print(f"Ocorreu um erro: {e}")

finally:
    if 'driver' in locals() and driver:
        driver.close()
        print("Conexão fechada.")