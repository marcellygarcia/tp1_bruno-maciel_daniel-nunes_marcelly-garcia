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
            select to_char(r.data::DATE,'dd/mm/yyyy') , to_char(avg(r.classficacao), '99.99') as media  from reviews r
            left join produto p on p.asin = r.asinproduto 
            where asinproduto = %s
            group by 1
            order by 1
        """, 
        """
            select x.pgroup,x.pos,x.asin,x.salesrank from 
        (select 
            row_number () over (partition by pgroup order by pgroup) as pos,
                p2.asin,p2.pgroup,salesrank 
            from
                produto p2
            where p2.salesrank >0
            order by p2.salesrank
        ) x
        where x.pos<=10
        order by x.pgroup,x.pos,x.salesrank

        """,
        """
            select
            p2.asinproduto,avg(p2.classficacao) 
            from
                reviews p2
            where p2.classficacao > 0 and util > 0 and votos > 0
            group by asinproduto
            order by 2 desc
            limit 10
	
        """,
        """
            Consulta 6
        """,
        """
            select x.pgroup,x.pos,x.cliente,x.num_comentarios from 
            (select 
                row_number () over (partition by pgroup order by count(p2.id) desc) as pos,
                p2.cliente,p3.pgroup, count(p2.id) as num_comentarios
                from
                    reviews p2
                left join produto p3 on p3.asin = p2.asinproduto 
                group by 3,2
                order by 3,4 desc,1,2
            ) x
            where x.pos<=10
            order by x.pgroup,x.num_comentarios desc,x.pos,x.cliente
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
   option = 0   
   while(option!=8):
    print("------------------- Trabalho Prático - Bancos de Dados I -------------------")
    print()
    print("1. Listar os 5 comentários mais úteis e com maior avaliação e os 5 comentários mais úteis e com menor avaliação")
    print("2. Listar os produtos similares com maiores vendas do que ele")
    print("3. Mostrar a evolução diária das médias de avaliação ao longo de um intervalo")
    print("4. Listar os 10 produtos líderes de venda em cada grupo de produtos")
    print("5. Listar os 10 produtos com a maior média de avaliações úteis positivas por produto")
    print("6. Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto")
    print("7. Listar os 10 clientes que mais fizeram comentários por grupo de produto")
    print("8. Sair")
    
    print()
    option = int(input("Digite a opção desejada: "))
    param = ""
    print("Opção",str(option),"selecionada")
    if option in [1,2,3]:
        param = input("Produto desejado: ")
    get_info(int(option),param)
