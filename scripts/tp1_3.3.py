import psycopg2

params = "dbname=postgres host=localhost user=postgres password=postgres port=5432"


commands = (
        """
           Consulta 1
        """,
        """ 
           Consulta 2
        """,
        """
            Consulta 3
        """,
        """
            Consulta 4
        """,
        """
            Consulta 5
        """,
        """
            Consulta 6
        """,
        """
            Consulta 7
        """)
def get_info(commandCode : int):
    conn = None
    try:
        conn = psycopg2.connect(params) 
        cur = conn.cursor();
        cur.execute(commands[commandCode-1])
        
        row = cur.fetchone()
        while row is not None:
            print(row)
            row = cur.fetchone()
        
        cur.close()
    except (Exception, pyscopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None: 
            conn.close()


if __name__ == '__main__':
   
    print("------------------- Trabalho Prático - Bancos de Dados I -------------------")
    print()
    print("1. Listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação")
    print("2. Listar os produtos similares com maiores vendas do que ele")
    print("3. Mostrar a evolução diária das médias de avaliação ao longo de um intervalo")
    print("4. Listar os 10 produtos líderes de venda em cada grupo de produtos")
    print("5. Listar os 10 produtos com a maior média de avaliações úteis positivas por produto")
    print("6. Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto")
    print("7. Listar os 10 clientes que mais fizeram comentários por grupo de produto")
    print()
    option = input("Digite a opção desejada: ")

