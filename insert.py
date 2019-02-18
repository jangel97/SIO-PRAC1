#!/usr/bin/python
import psycopg2 


def getFile():
    with open("prac1_events_hospital.csv","r") as ins:
        for line in ins:
            arr=map(lambda x:x.rstrip(),line.split(';'))    #coge sin saltos de linea ni nada
            insert("metge","metgeid","'"+arr[2]+"'")
            insert("esdeveniment","esdevenimentid,metgeid,accio,pacientid,pacientedat,timestamp","'"+arr[0]+"'"+','+"'"+arr[2]+"'"+','+"'"+arr[5]+"'"+','+"'"+arr[3]+"'"+','+"'"+arr[4]+"'"+','+"'"+arr[6]+"'")
            insert("episodi","episodiid,esdevenimentid","'"+arr[1]+"'"+",'"+arr[0]+"'")
def connect():
    conn = None 
    try:
        conn = psycopg2.connect("host=localhost dbname=sio1 user=postgres password=postgres")

# create a cursor
        cur = conn.cursor()
        
 # execute a statement
        try:
            print('PostgreSQL database version:')
            cur.execute('SELECT version()')
        except:
            print 'no pudo coger la version de la db'


        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)
        '''    
        try:
            cur.execute("INSERT INTO role (role_id,role_name) VALUES (12,23);")
            conn.commit()    #enviar cambios a la db
        except (Exception, psycopg2.DatabaseError) as error:
            print (error)
        '''    
        try:
            print 'queryyy'
            cur.execute("SELECT * FROM role ")
            print cur.fetchall()
        except (Exception, psycopg2.DatabaseError) as error:
            print (error)



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
     connection = psycopg2.connect("host=localhost dbname=sioPRACTICA1 user=postgres password=postgres")
     try:  
        mark = connection.cursor()
        statement = "INSERT INTO " + table + " (" + columns + ") VALUES ( "+ values + " );"
        mark.execute(statement)
        connection.commit()
     except (Exception, psycopg2.DatabaseError) as error:
         print error

     return 

if __name__ == '__main__':
    #connect()
    getFile()
