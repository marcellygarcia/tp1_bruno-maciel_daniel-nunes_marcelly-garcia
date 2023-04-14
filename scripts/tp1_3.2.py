import psycopg2

params = "dbname=postgres host=localhost user=postgres password=postgres port=5432"


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """
           TABELA 1
        """,
        """ 
           TABELA 2
        """,
        """
            TABELA 3
        """,
        """
            TABELA 4
        """,
        """
            TABELA 5
        """,
        """
            TABELA 6
        """,
        """
            TABELA 7
        """)
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
        print("Categorories: ",categories)
        print("Reviews: ", reviews)
        


    print()
if __name__ == '__main__':
    read_entry("teste.txt")