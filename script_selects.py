#!/usr/bin/python
import psycopg2 

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
           
        try:

	    print 'Query'
	    print 'Comptatges'
            print 'Num_metges'
            cur.execute("SELECT COUNT (*) FROM metge;")
            print cur.fetchall()
	
	    print 'Num_pacients'
            cur.execute("SELECT COUNT (DISTINCT pacientid) FROM esdeveniment;")
            print cur.fetchall()

	    print 'Num_esdeveniments_total'
            cur.execute("SELECT COUNT (DISTINCT esdevenimentid) FROM esdeveniment;")
            print cur.fetchall()

	    print 'Num_esdeveniments_metge_particular'
            cur.execute("SELECT COUNT (DISTINCT esdevenimentid) FROM esdeveniment WHERE metgeid='M1';")
            print cur.fetchall()

	    print 'Num_esdeveniments_pacient_particular'
            cur.execute("SELECT COUNT (DISTINCT esdevenimentid) FROM esdeveniment WHERE pacientid='P1';")
            print cur.fetchall()

	    print 'Seleccionar_edat_pacient_particular'
            cur.execute("SELECT DISTINCT pacientedat FROM esdeveniment WHERE pacientid='P1';")
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

if __name__ == '__main__':
    connect()

#EDAT MITJANA PACIENTS HOSPITAL

#SELECT AVG(pacientedat) FROM esdeveniment WHERE (SELECT);

#relacionar id unic amb la edat:
#SELECT DISTINCT ON (pacientid) pacientid,pacientedat FROM esdeveniment ORDER BY pacientid,pacientedat;
#conjunt unic de edats:
#SELECT DISTINCT ON (pacientid) pacientedat FROM esdeveniment;


#creem vista (edat mitjana dels pacients de l'hospital sense repetits):
#create view my_data1 AS with data as (SELECT DISTINCT ON (pacientid) pacientedat FROM esdeveniment)
#SELECT AVG (pacientedat) FROM data;
#SELECT * FROM my_data1;

#MITJA DE COPS QUE UN PACIENT ES FA UNA PROVA





