import psycopg2
import sys

params = "dbname=postgres host=localhost user=postgres password=postgres port=5432"
tables = ("reviews","produtoSimilar","produtoCategoria","categoria","produto")
commands = (
        """
           CREATE TABLE produto (
                asin VARCHAR(10) PRIMARY KEY,
                title VARCHAR(470) NOT NULL,
                pgroup varchar(30) NOT NULL,
                salesrank INTEGER NOT NULL
           )
        """,
        """ 
           CREATE TABLE categoria(
             id INTEGER PRIMARY KEY,
             name VARCHAR(250)
           )
        """,
        """
           CREATE TABLE produtoCategoria(
                id INTEGER not NULL,
                sequencia INTEGER NOT NULL,
                asinproduto VARCHAR(10),
                id_categoria INTEGER NOT null,
                primary key (id,sequencia),
                CONSTRAINT fk_prodcateg_asinproduto
                    FOREIGN KEY(asinproduto)
                            REFERENCES produto (asin),
                
                CONSTRAINT fk_prodcateg_categoria
                    FOREIGN KEY(id_categoria)
                            REFERENCES categoria (id)
           )

        """,
        """
            CREATE TABLE produtoSimilar(
              id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
              asinproduto VARCHAR(10),
              asinsimilar VARCHAR(10),
              CONSTRAINT fk_prodsim_asinproduto
	              FOREIGN KEY(asinproduto)
	                    REFERENCES produto (asin),
	          
              CONSTRAINT fk_prodsim_asinsimilar
	              FOREIGN KEY(asinsimilar)
	                    REFERENCES produto (asin)
	              
           )

        """,
        """
            CREATE TABLE reviews (
              id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
              asinproduto VARCHAR(10),
              data DATE DEFAULT CURRENT_DATE,
              cliente VARCHAR(20) not null,
              classficacao INTEGER DEFAULT 0,
              votos INTEGER DEFAULT 0, 
              util INTEGER DEFAULT 0,
              CONSTRAINT fk_reviews_asinproduto
	              FOREIGN KEY(asinproduto)
	                    REFERENCES produto (asin)
	          
            )
        """)
def remove_tables():
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(params)
        cur = conn.cursor()
        # create table one by one
        for table in tables:
            #print("DROP TABLE IF EXISTS %s",table)
            cur.execute("DROP TABLE IF EXISTS "+table+" ")
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
        
def create_tables():
    """ create tables in the PostgreSQL database"""
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(params)
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def read_entry(entryName):
    arq = open(entryName,"r")
    data = arq.read();
    dataList = data.split('\n\n')
    for ndata in dataList:
        print(ndata)
        print('--------------------------------------------------------------------------------')
        lines = ndata.split('\n');
        #print(lines)
        title =""
        asin = ""
        group = ""
        salesrank = ""
        similar = []
        categories = []
        reviews=[]
        for line in lines:
            if ']|' in line:
                info = line.split('|')
                categories.append(info[1::])
            elif 'title' in line:
                    info = line.split(' ',3)
                    print(info)
                    while '' in info:
                        info.remove('')
                    title = info[1]
                    print(info)
                    
            else:
                info = line.split(" ");
                ##
                while '' in info:
                    info.remove('')
                
                #####
                
                if 'ASIN' in line:
                    asin = info[1]
                if 'group' in line:
                    group = info[1]
                if 'salesrank' in line:
                    salesrank = info[1]
                if 'similar' in line:
                    for n in info[2:]:
                        similar.append(n)
                if 'cutomer' in line:
                    reviews.append(info[0::2])
                
        print("ASIN: ",asin)
        print("Tittle: ",title)
        print("Group: ", group)
        print("Sales Rank: ", salesrank)            
        print("Similares: ",similar)
        print("Categories: ",categories)
        print("Reviews: ", reviews)
        


    print()
if __name__ == '__main__':
    fileName = sys.argv[1]
    #print(fileName)
    read_entry(fileName)
    #remove_tables()
    #create_tables()