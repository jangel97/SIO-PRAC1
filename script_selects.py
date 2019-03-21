#!/usr/bin/python
import psycopg2 
import operator
#import tkinter



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
	
	    print 'Accions diferents que es poden realitzar'
            cur.execute("SELECT DISTINCT accio FROM esdeveniment;")
            lista3 = map(lambda x: (x[0]),cur.fetchall())
	    print lista3
	    

	    print 'pacients diferents'
            cur.execute("SELECT COUNT (DISTINCT pacientid) FROM esdeveniment;")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num_esdeveniments_total'
            cur.execute("SELECT COUNT (DISTINCT esdevenimentid) FROM esdeveniment;")
	    numesdeveniments=map(lambda x: int(x[0]),cur.fetchall())[0]
            print numesdeveniments

	    print 'Num_esdeveniments_metge_particular'
            cur.execute("SELECT COUNT (DISTINCT esdevenimentid) FROM esdeveniment WHERE metgeid='M1';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num_esdeveniments_pacient_particular'
            cur.execute("SELECT COUNT (DISTINCT esdevenimentid) FROM esdeveniment WHERE pacientid='P4';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Seleccionar_edat_pacient_particular'
            cur.execute("SELECT DISTINCT pacientedat FROM esdeveniment WHERE pacientid='P1';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Seleccionar_cont_per_un_pacient'
            cur.execute("SELECT COUNT (*) FROM esdeveniment WHERE pacientid='P1';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Seleccionar_cont_per_un_metge'
            cur.execute("SELECT COUNT (*) FROM esdeveniment WHERE metgeid='M2';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Mitjana de esdeveniments respecte els metges'
	    cur.execute("SELECT COUNT (*) FROM esdeveniment WHERE metgeid IN (SELECT metgeid FROM metge) GROUP BY metgeid;")	
	    lista = map(lambda x: int(x[0]),cur.fetchall())
	    print reduce(lambda x, y: x+y, lista)/len(lista)

	    print 'Mitjana de esdeveniments respecte els pacients'
	    cur.execute("SELECT COUNT (*) FROM esdeveniment WHERE pacientid IN (SELECT DISTINCT pacientid FROM esdeveniment) GROUP BY pacientid;")	
	    lista2 = map(lambda x: int(x[0]),cur.fetchall())
	    print reduce(lambda x, y: x+y, lista2)/len(lista2)
		
	    print 'Mitjana de esdeveniments respecte les accions'
	    cur.execute("SELECT COUNT (*) FROM esdeveniment WHERE accio IN (SELECT DISTINCT accio FROM esdeveniment) GROUP BY accio;")	
	    lista3 = map(lambda x: int(x[0]),cur.fetchall())
	    print reduce(lambda x, y: x+y, lista3)/len(lista3)

	    print 'Accio que mes es repeteix de 0 a 5 anys'
	    cur.execute("SELECT accio FROM esdeveniment WHERE pacientedat < 6;")
	    lista4 = map(lambda x: (x[0]),cur.fetchall())
	    valormax5 = max(lista4)
	    print valormax5

	    print 'Accio que mes es repeteix de 6 a 18 anys'
	    cur.execute("SELECT accio FROM esdeveniment WHERE pacientedat < 19 AND pacientedat > 5;")
	    lista5 = map(lambda x: (x[0]),cur.fetchall())
	    valormax18 = max(lista5)
	    print valormax18

	    print 'Accio que mes es repeteix de 19 a 45 anys'
	    cur.execute("SELECT accio FROM esdeveniment WHERE pacientedat < 46 AND pacientedat > 18;")
	    lista6 = map(lambda x: (x[0]),cur.fetchall())
	    valormax45 = max(lista6)
	    print valormax45

	    print 'Accio que mes es repeteix de 46 a 65 anys'
	    cur.execute("SELECT accio FROM esdeveniment WHERE pacientedat < 66 AND pacientedat > 45;")
	    lista7 = map(lambda x: (x[0]),cur.fetchall())
	    valormax65 = max(lista7)
	    print valormax65

	    print 'Accio que mes es repeteix de 66 a 80 anys'
	    cur.execute("SELECT accio FROM esdeveniment WHERE pacientedat < 81 AND pacientedat > 65;")
	    lista8 = map(lambda x: (x[0]),cur.fetchall())
	    valormax80 = max(lista8)
	    print valormax80

	    print 'Accio que mes es repeteix de 81 a 100 anys'
	    cur.execute("SELECT accio FROM esdeveniment WHERE pacientedat < 101 AND pacientedat > 80;")
	    lista9 = map(lambda x: (x[0]),cur.fetchall())
	    valormax100 = max(lista9)
	    print valormax100

	    print 'Numero de cops que sha realitzat cada accio'
	    accions_metge={}
            cur.execute("SELECT DISTINCT accio FROM esdeveniment")
            llista_accions=map(lambda x: x[0],cur.fetchall())
            accions={}
            for accio in llista_accions:
             cur.execute("SELECT COUNT(*) FROM esdeveniment WHERE accio='"+accio+"'")
             accions[accio]=map(lambda x: int(x[0]),cur.fetchall())[0]
             llista_accions_endrecades = list(reversed(sorted(accions.items(),key=operator.itemgetter(1))))
		
	    #print 'Omplir diccionaris daccions i despecialitat amb nombre de cops que hem fet les accions i fent insert a metge especialitat'
	    #for i in range(1,301):   
             #cur.execute("SELECT DISTINCT accio FROM esdeveniment WHERE metgeid='M"+str(i)+"'")
	     #val={}
             #llista_accions=map(lambda x: x[0],cur.fetchall())
             #print str(llista_accions)
             #if 'Enguixar extremitat' in llista_accions:
             #	especialitat="Traumatoleg"
             #elif 'Radiografia' in llista_accions:
             #	especialitat="Radioleg"
             #elif 'Electrocardiograma' in llista_accions:
             #	especialitat="Cardioleg"
             #elif 'Donar piruleta' in llista_accions:
             #	especialitat="Pediatra"
             #else:
             #	especialitat="Metge general"
	     #print especialitat

             #cur.execute("UPDATE metge SET especialitat = '"+especialitat+"' WHERE metgeid ='M"+str(i)+"';")
             #conn.commit()

             #accions_metge['M'+str(i)]={'especialitat':especialitat}
	     #print str(accions_metge)
	
	     
	
	    print 'Num Traumatolegs'
            cur.execute("SELECT COUNT (*) FROM metge WHERE especialitat='Traumatoleg';")
	    aux=map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num Radiolegs'
            cur.execute("SELECT COUNT (*) FROM metge WHERE especialitat='Radioleg';")
	    aux2=map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num Cardiolegs'
            cur.execute("SELECT COUNT (*) FROM metge WHERE especialitat='Cardioleg';")
	    aux3=map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num Pediatra'
            cur.execute("SELECT COUNT (*) FROM metge WHERE especialitat='Pediatra';")
	    aux4=map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num Metge General'
            cur.execute("SELECT COUNT (*) FROM metge WHERE especialitat='Metge general';")
	    aux5=map(lambda x: int(x[0]),cur.fetchall())[0]

	    percentatges=[aux*100/300,aux2*100/300,aux3*100/300,aux4*100/300,aux5*100/300]
	    print percentatges

            #import numpy as np
	    #import matplotlib.pyplot as plt
	    #from matplotlib.ticker import PercentFormatter

	    #plt.hist(percentatges, weights=np.ones(len(percentatges)) / len(percentatges))

	    #plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
	    #plt.show()


	    taula2=[]
            cur.execute("SELECT DISTINCT accio FROM esdeveniment")
            for accio in map(lambda x: x[0],cur.fetchall()):
	     cur.execute("SELECT COUNT(DISTINCT  metgeid)  FROM esdeveniment WHERE  accio='"+accio+"'")
             print "L'accio "+ accio + ", es feta per " + str(map(lambda x: int(x[0]),cur.fetchall())[0]) + " metges diferents"
             cur.execute("SELECT COUNT(*)  FROM esdeveniment WHERE  accio='"+accio+"'")
	     accio=str(map(lambda x:int(x[0]), cur.fetchall())[0])
             print "L'accio "+ accio + ", s'ha fet: " + accio + " cops"
	     taula2.append(float(accio)*100/numesdeveniments)
	    print taula2

	    print 'Obtenim ledat dels 100 pacients que mes esdeveniments han tingut'


	    cur.execute("SELECT pacientid,pacientedat, COUNT (pacientid) AS mostfrequent FROM esdeveniment GROUP BY pacientid,pacientedat ORDER BY COUNT (pacientid) DESC limit 100;")
	    pacientsordenats=cur.fetchall()
	    print pacientsordenats

            cur.execute("SELECT pacientid,pacientedat, COUNT (pacientid) AS mostfrequent FROM esdeveniment GROUP BY pacientid,pacientedat ORDER BY COUNT (pacientid) DESC limit 100;")
	    pacientsordenats=reduce(lambda x,y: x+y,(map(lambda x: x[1],cur.fetchall())))
	    print (float(pacientsordenats)/100)

	    #creem vista (edat mitjana dels pacients de l'hospital  sense repetits):
 
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

#ACCIO MES REPETIDA PER RANG D'EDAT



#DROP VIEW IF EXISTS nom_taula;


