import psycopg2
import sys
import gc
import time
params = "dbname=postgres host=localhost user=postgres password=postgres port=5432"
tables = ("reviews","produtoSimilar","produtoCategoria","categoria","produto")
similars = []
nreviews = []
ncategories = []
nproducts = []
idCateg = [1]
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
        print("Removing tables created before")
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
        print("Tables Removed")
        
       
def create_tables():
    """ create tables in the PostgreSQL database"""
    conn = None
    try:
        # connect to the PostgreSQL server
        conn = psycopg2.connect(params)
        cur = conn.cursor()
        # create table one by one
        print("Creating Tables")
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
        print("Tables Created")

def process_similar():
    try:
        conn = psycopg2.connect(params)
        cur = conn.cursor()
        # create table one by one
        command = ""
        icount = 0
        for count,nsimilar in enumerate(similars,start=1):
            if nsimilar != []: 
                for item in nsimilar[1::] :
                    #print(nsimilar)
                    command += "INSERT INTO produtoSimilar(asinproduto,asinsimilar) values ('"+nsimilar[0]+"','"+item+"');\n"
                    print("Processing similars: ",count,item)
                    icount+=1
                    if((icount)%50000==0):
                        cur.execute(command)
                        command=""
                # close communication with the PostgreSQL database server
        if command != "":
            cur.execute(command)
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit()
    finally:
        if conn is not None:
            conn.close()

def process_reviews():
    try:
        conn = psycopg2.connect(params)
        cur = conn.cursor()
        conn.autocommit=True
        # create table one by one
        command=""
        for count,x in enumerate(nreviews,start=1):
            #print(x)
            if x != []: 
                asin= x[0]
                data = x[1]
                cliente = x[2]
                classificacao = x[3]
                votos= x[4]
                util = x[5]
                #print(item)
                command += "INSERT INTO reviews(asinproduto,data,cliente,classficacao,votos,util) values ('"+asin+"','"+data+"','"+cliente+"',"+classificacao+","+votos+","+util+");\n"
                print("Process Reviews:", count)
                if(count%50000 == 0):
                    print("Executing")
                    cur.execute(command)
                    command = ""                    
        cur.execute(command)
        cur.close()
        # commit the changes
        conn.commit()
        nreviews.clear();
        gc.collect()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit()
    finally:
        if conn is not None:
            conn.close()

def process_products():
    try:
        conn = psycopg2.connect(params)
        conn.autocommit = True
        cur = conn.cursor()
        command = ""
        for count,product in enumerate(nproducts,start=1):
            asin = product[0]
            group = product[1]
            title = product[2]
            salesrank = product[3]
            # create table one by one
            command+= "INSERT INTO produto(asin,pgroup,title,salesrank) values ('"+asin+"', '"+group+"','"+title+"',"+salesrank+");\n"
            #print("Process Product:",count)
            # close communication with the PostgreSQL database server
        cur.execute(command)
        cur.close()
        # commit the changes
        conn.commit()
        nproducts.clear()
        gc.collect()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit()
    finally:
        if conn is not None:
            conn.close()

def process_categories():
    try:
        conn = psycopg2.connect(params)
        cur = conn.cursor()
        # create table one by one
        command = ""
        count = 0
        for categories in ncategories:
            #print(categories)
            asin=categories[0]
            for index,item in enumerate(categories[1:],start=1) :
                #print(item)
                items = item.split('[')
                command += "INSERT INTO categoria(id,name) values ("+items[-1].replace("]","")+", '"+items[0]+"') ON CONFLICT (id) DO NOTHING;; \n"
                command += "INSERT INTO produtoCategoria(id,sequencia,asinproduto,id_categoria) values ("+str(len(idCateg))+","+str(index)+",'"+asin+"',"+items[-1].replace("]","")+");\n"
                print("Processing Categories: ",count)
                count+=2  
                if ((count%50000) == 0):
                    print("Executing")
                    cur.execute(command)
                    count = 0
                    #time.sleep(10)
                    command="" 
            idCateg.append(len(idCateg)+1)
            #print(idCateg)
            #time.sleep(10)
        cur.execute(command)    
        cur.close()
        # commit the changes
        conn.commit()
        ncategories.clear()
        gc.collect()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        sys.exit()
    finally:
        if conn is not None:
            conn.close()
             


def read_entry(entryName):
    arq = open(entryName,"r")
    data = arq.read();
    dataList = data.split('\nId:   ')
    #print(dataList)
    for count,ndata in enumerate(dataList,start=1):
        #print(ndata)
        #print("")
        #print('--------------------------------------------------------------------------------')
        lines = ndata.split('\n');
        #print(lines)
        title =""
        asin = ""
        group = ""
        salesrank = ""
        for line in lines:
            if ']|' in line:
                line = line.replace("'","''")
                info = line.split('|')
                ncategories.append([asin]+info[1::])
            elif 'title' in line:
                    info = line.split(' ',3)
                    #print(line)
                    while '' in info:
                        info.remove('')
                    title = info[1]
                   # print(info)
                    
            else:
                info = line.split(" ");
                ##
                while '' in info:
                    info.remove('')
                
                #####
                
                if 'ASIN:' in line:
                    print(line)
                    asin = info[1]
                if 'group:' in line:
                    group = info[1]
                if 'salesrank:' in line:
                    salesrank = info[1]
                if 'similar:' in line:
                    #similar.append(asin)
                    #for n in info[2:]:
                    #    similar.append(n)
                    similars.append([asin]+info[2::])
                if 'cutomer:' in line:
                    nreviews.append([asin]+info[0::2])
        print("Product:",count)  
        """if (asin=='B00004YWW7') or (asin == 'cutomer:'):
                print(ndata);
                #print(nproducts)
                sys.exit()    
         """   
        if title != '':
            title=title.replace("'","''") 
            nproducts.append([asin,group,title,salesrank])
            #similars.append(similar)
            #nreviews.append()
            if len(nproducts)==20000:
               process_products()     
               process_reviews()
               process_categories()
               gc.collect()

        #print(asin)
        #print(title)
        #print(group)
        #print(salesrank)
        #print(nproducts)
        #print(nreviews)


                       
     #  process_reviews()
     #          nreviews.clear() 
    print()

if __name__ == '__main__':
    fileName = sys.argv[1]
    tinicio = time.time()
    print(fileName)
    remove_tables()
    create_tables()
    read_entry(fileName)
    print("Incluindo Produtos")
    process_products()
    print("Incluindo Reviews")
    process_reviews()
    print("Incluindo Produtos Similares")
    print(similars)
    process_similar()
    print("Incluindo Produtos Categorias e subcategorias")
    process_categories()
    tfim = time.time()
    print("Tempo de execução:",(tfim-tinicio,"segundos"))