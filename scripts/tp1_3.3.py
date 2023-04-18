import psycopg2

params = "dbname=postgres host=localhost user=postgres password=postgres port=5432"

codprod = ""
commands = (
        """
           (select r.* from reviews r where r.asinproduto = %s order by r.util desc,r.votos desc limit 5)
            union all
           (select r.* from reviews r where r.asinproduto = %s order by r.util desc,r.votos asc limit 5) 

        """,
        """ 
            select p.* from produtosimilar p2 
            left join produto p on p.asin = p2.asinsimilar 
            left join produto p4 on p4.asin = p2.asinproduto 
            where p2.asinproduto = %s and p.salesrank < (select p3.salesrank from produto p3 where asin = p2.asinproduto)

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

def get_info(commandCode : int,param):
    conn = None
    try:
        conn = psycopg2.connect(params) 
        cur = conn.cursor();
        command = commands[commandCode-1]
        if (commandCode in [1,2,3]):
            command = command.replace("%s","'"+str(param)+"'")
        cur.execute(command,{param,param})
        
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
    option = int(input("Digite a opção desejada: "))
    param = ""
    print("Opção",str(option),"selecionada")
    if option in [1,2,3]:
        param = input("Produto desejado: ")
    get_info(int(option),param)
