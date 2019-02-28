#!/usr/bin/python
import psycopg2 
import operator

def getFile():
    with open("prac1_events_hospital.csv","r") as ins:
        for line in ins:
            arr=map(lambda x:x.rstrip(),line.split(';'))    #coge sin saltos de linea ni nada
            #insert("metge","metgeid","'"+arr[2]+"'")
            insert("esdeveniment","esdeveniment_id,episodi_id,metge_id,id_pacient,edad_pacient,accio,data","'"+arr[0]+"'"+','+"'"+arr[1]+"'"+','+"'"+arr[2]+"'"+','+"'"+arr[3]+"'"+','+"'"+arr[4]+"'"+','+"'"+arr[5]+"'"+','+"'"+arr[6] + "'")
            #insert("episodi","episodiid,esdevenimentid","'"+arr[1]+"'"+",'"+arr[0]+"'")
def connect():
    conn = None 
    try:
        conn = psycopg2.connect("host=localhost dbname=sioprac1 user=postgres password=postgres")

# create a cursor
        cur = conn.cursor()
        
 # execute a statement
        #for especialitat in ["Radiografia",""]
        accions_metge={}
        cur.execute("SELECT DISTINCT accio FROM esdeveniment")
        llista_accions=map(lambda x: x[0],cur.fetchall())
        accions={}
        for accio in llista_accions:
            cur.execute("SELECT COUNT(*) FROM esdeveniment WHERE accio='"+accio+"'")
            accions[accio]=map(lambda x: int(x[0]),cur.fetchall())[0]
        llista_accions_endrecades = list(reversed(sorted(accions.items(),key=operator.itemgetter(1))))
        print 'Accions: '+ str(accions)
        for i in range(1,3):   
         cur.execute("SELECT DISTINCT accio FROM esdeveniment WHERE metge_id='M"+str(i)+"'")
         val={}
         llista_accions=map(lambda x: x[0],cur.fetchall())
         for accio in llista_accions:  #contar quants cops fa una accio el metge
          cur.execute("SELECT COUNT(*)  FROM esdeveniment WHERE metge_id='M"+str(i)+"'" + " AND accio='"+ accio + "'" )
          val[accio]=map(lambda x:int(x[0]), cur.fetchall())[0]
         
         especialitat=max(val.iteritems(),key=operator.itemgetter(1))[0]
         print especialitat
         try:
          cur.execute("INSERT INTO metge (metge_id, especialitat) VALUES ('M"+str(i)+"','"+especialitat+"');")
          
          conn.commit()
         
         except (Exception, psycopg2.DatabaseError) as error:
             print error
         cur.execute("SELECT COUNT(*) FROM esdeveniment  WHERE accio='"+especialitat+"'")
         accions_metge['M'+str(i)]={'accions':val,'maxim':especialitat}
        print str(accions_metge)
        print '--------------------------------------------------------'
        cur.execute("SELECT DISTINCT accio FROM esdeveniment")
        for accio in map(lambda x: x[0],cur.fetchall()):
         cur.execute("SELECT COUNT(DISTINCT  metge_id)  FROM esdeveniment WHERE  accio='"+accio+"'")
         print "L'accio "+ accio + ", es feta per " + str(map(lambda x: int(x[0]),cur.fetchall())[0]) + " metges diferents"
        
         cur.execute("SELECT COUNT(*)  FROM esdeveniment WHERE  accio='"+accio+"'")
         print "L'accio "+ accio + ", s'ha fet: " + str(map(lambda x:int(x[0]), cur.fetchall())[0]) + " cops"
         



     # close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
    #except:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')
 
 

'''
table = "Owls"
fields = "id, kind, date"
values = "17965, Barn owl, 2006-07-16"
insert(type, fields, values) 
'''
def insert(table, columns, values):
     connection = psycopg2.connect("host=localhost dbname=sioprac1 user=postgres password=postgres")
     try:  
        mark = connection.cursor()
        #statement = "INSERT INTO " + table + " (" + columns + ") VALUES ( "+ values + " );"
        mark.execute(statement)
        connection.commit()
     except (Exception, psycopg2.DatabaseError) as error:
         print error

     return 

if __name__ == '__main__':
    #getFile()
     connect()
