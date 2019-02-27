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
            print map(lambda x: int(x[0]),cur.fetchall())[0]
	
	    print 'Num_pacients'
            cur.execute("SELECT COUNT (DISTINCT pacientid) FROM esdeveniment;")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num_esdeveniments_total'
            cur.execute("SELECT COUNT (DISTINCT esdevenimentid) FROM esdeveniment;")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num_esdeveniments_metge_particular'
            cur.execute("SELECT COUNT (DISTINCT esdevenimentid) FROM esdeveniment WHERE metgeid='M1';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num_esdeveniments_pacient_particular'
            cur.execute("SELECT COUNT (DISTINCT esdevenimentid) FROM esdeveniment WHERE pacientid='P1';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Seleccionar_edat_pacient_particular'
            cur.execute("SELECT DISTINCT pacientedat FROM esdeveniment WHERE pacientid='P1';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Seleccionar_cont_per_un_pacient'
            cur.execute("SELECT COUNT (*) FROM esdeveniment WHERE metgeid='M1';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Seleccionar_cont_per_un_pacient'
            cur.execute("SELECT COUNT (*) FROM esdeveniment WHERE metgeid='M1';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Mitjana de esdeveniments respecte els metges'
	    cur.execute("SELECT COUNT (*) FROM esdeveniment WHERE metgeid IN (SELECT metgeid FROM metge) GROUP BY metgeid;")	
	    lista = map(lambda x: int(x[0]),cur.fetchall())
	    print reduce(lambda x, y: x+y, lista)/len(lista)
	
	    

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




#MITJA DE ESDEVENIMENT PER RANGS D'EDAT
#0-5 SELECT COUNT (*) FROM esdeveniment WHERE pacientedat < 6;
#6-18 create view sisdivuit AS with dades1 as (SELECT * FROM esdeveniment WHERE pacientedat > 5)
#SELECT COUNT (*) FROM dades1 WHERE pacientedat < 19;
#SELECT * FROM sisdivuit;

#19-35 create view dinoutrentacinc AS with dades2 as (SELECT * FROM esdeveniment WHERE pacientedat > 18)
#SELECT COUNT (*) FROM dades2 WHERE pacientedat < 36;
#SELECT * FROM dinoutrentacinc;

#36-60 create view trentasisseixanta AS with dades3 as (SELECT * FROM esdeveniment WHERE pacientedat > 35)
#SELECT COUNT (*) FROM dades3 WHERE pacientedat < 61;
#SELECT * FROM trentasisseixanta;

#61-100 create view seixantaucent AS with dades4 as (SELECT * FROM esdeveniment WHERE pacientedat > 60)
#SELECT COUNT (*) FROM dades4 WHERE pacientedat < 101;
#SELECT * FROM seixantaucent;

#creat taula edats i la taula per un rang d'edat amb tots els numeros desdeveniments de tots els pacients(relacio entre pacient i en num de esdeveniment en el que surt)
#create view prova AS with dades as (SELECT pacientedat FROM esdeveniment)SELECT * FROM dades;


# num esdeveniments per metge relacio
#SELECT metgeid, COUNT (*) FROM esdeveniment WHERE metgeid IN (SELECT metgeid FROM metge) GROUP BY metgeid;





#DROP VIEW IF EXISTS nom_taula;


