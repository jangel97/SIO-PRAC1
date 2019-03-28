#!/usr/bin/python
import psycopg2 
import operator
#import tkinter



def connect():
    conn = None 
    try:
        conn = psycopg2.connect("host=localhost dbname=siodefinitiva user=postgres password=postgres")

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
            cur.execute("SELECT COUNT (*) FROM METGE;")
            print map(lambda x: int(x[0]),cur.fetchall())[0]
	    
	    print 'Accions diferents que es poden realitzar'
            cur.execute("SELECT DISTINCT ACCIO FROM esdeveniments2;")
            lista3 = map(lambda x: (x[0]),cur.fetchall())
	    print lista3
	    

	    print 'pacients diferents'
            cur.execute("SELECT COUNT (DISTINCT PACIENT) FROM esdeveniments2;")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num_esdeveniments_total'
            cur.execute("SELECT COUNT (DISTINCT ESDEVENIMENT) FROM esdeveniments2;")
	    numesdeveniments=map(lambda x: int(x[0]),cur.fetchall())[0]
            print numesdeveniments

	    print 'Num_esdeveniments_metge_particular'
            cur.execute("SELECT COUNT (DISTINCT ESDEVENIMENT) FROM esdeveniments2 WHERE METGE='M1';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num_esdeveniments_pacient_particular'
            cur.execute("SELECT COUNT (DISTINCT ESDEVENIMENT) FROM esdeveniments2 WHERE PACIENT='P4';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Seleccionar_edat_pacient_particular'
            cur.execute("SELECT DISTINCT EDAT FROM esdeveniments2 WHERE PACIENT='P1';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Seleccionar_cont_per_un_pacient'
            cur.execute("SELECT COUNT (*) FROM esdeveniments2 WHERE PACIENT='P1';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Seleccionar_cont_per_un_metge'
            cur.execute("SELECT COUNT (*) FROM esdeveniments2 WHERE METGE='M2';")
            print map(lambda x: int(x[0]),cur.fetchall())[0]
	    
	    print 'Mitjana de esdeveniments respecte els metges'
	    cur.execute("SELECT COUNT (*) FROM esdeveniments2 WHERE METGE IN (SELECT METGE FROM METGE) GROUP BY METGE;")	
	    
	    lista = map(lambda x: int(x[0]),cur.fetchall())
	    print "LItsaaaaaaaaaaaaaaaa: "+str(lista)
	    print sum(lista)/len(lista)
	    
	    print 'Mitjana de esdeveniments respecte els pacients'
	    cur.execute("SELECT COUNT (*) FROM esdeveniments2 WHERE PACIENT IN (SELECT DISTINCT(PACIENT) FROM ESDEVENIMENTS2) GROUP BY PACIENT;")	
	    lista2 = map(lambda x: int(x[0]),cur.fetchall())
	    print lista2
	    print reduce(lambda x, y: x+y, lista2)/len(lista2)
		
	    print 'Mitjana de esdeveniments respecte les accions'
	    cur.execute("SELECT COUNT (*) FROM esdeveniments2 WHERE ACCIO IN (SELECT DISTINCT ACCIO FROM ESDEVENIMENTS2) GROUP BY ACCIO;")	
	    lista3 = map(lambda x: int(x[0]),cur.fetchall())
	    print reduce(lambda x, y: x+y, lista3)/len(lista3)

	    print 'ACCIO que mes es repeteix de 0 a 5 anys'
	    cur.execute("SELECT ACCIO FROM esdeveniments2 WHERE EDAT < 6;")
	    lista4 = map(lambda x: (x[0]),cur.fetchall())
	    valormax5 = max(lista4)
	    print valormax5

	    print 'ACCIO que mes es repeteix de 6 a 18 anys'
	    cur.execute("SELECT ACCIO FROM esdeveniments2 WHERE EDAT < 19 AND EDAT > 5;")
	    lista5 = map(lambda x: (x[0]),cur.fetchall())
	    valormax18 = max(lista5)
	    print valormax18

	    print 'ACCIO que mes es repeteix de 19 a 45 anys'
	    cur.execute("SELECT ACCIO FROM esdeveniments2 WHERE EDAT < 46 AND EDAT > 18;")
	    lista6 = map(lambda x: (x[0]),cur.fetchall())
	    valormax45 = max(lista6)
	    print valormax45

	    print 'ACCIO que mes es repeteix de 46 a 65 anys'
	    cur.execute("SELECT ACCIO FROM esdeveniments2 WHERE EDAT < 66 AND EDAT > 45;")
	    lista7 = map(lambda x: (x[0]),cur.fetchall())
	    valormax65 = max(lista7)
	    print valormax65

	    print 'ACCIO que mes es repeteix de 66 a 80 anys'
	    cur.execute("SELECT ACCIO FROM esdeveniments2 WHERE EDAT < 81 AND EDAT > 65;")
	    lista8 = map(lambda x: (x[0]),cur.fetchall())
	    valormax80 = max(lista8)
	    print valormax80

	    print 'ACCIO que mes es repeteix de 81 a 100 anys'
	    cur.execute("SELECT ACCIO FROM esdeveniments2 WHERE EDAT < 101 AND EDAT > 80;")
	    lista9 = map(lambda x: (x[0]),cur.fetchall())
	    valormax100 = max(lista9)
	    print valormax100

	    print 'Numero de cops que sha realitzat cada ACCIO'
	    accions_metge={}
            cur.execute("SELECT DISTINCT ACCIO FROM esdeveniments2")
            llista_accions=map(lambda x: x[0],cur.fetchall())
            accions={}
            for ACCIO in llista_accions:
             cur.execute("SELECT COUNT(*) FROM esdeveniments2 WHERE ACCIO='"+ACCIO+"'")
             accions[ACCIO]=map(lambda x: int(x[0]),cur.fetchall())[0]
             llista_accions_endrecades = list(reversed(sorted(accions.items(),key=operator.itemgetter(1))))
	    
	    print 'Omplir diccionaris daccions i despecialitat amb nombre de cops que hem fet les accions i fent insert a METGE especialitat'
	    for i in range(1,126):
		cur.execute("UPDATE METGE SET especialitat = 'Truamatoleg' WHERE metgeid ='M"+str(i)+"';")
		conn.commit()
	    for i in range(126,211):
		cur.execute("UPDATE METGE SET especialitat = 'Cardioleg' WHERE metgeid ='M"+str(i)+"';")
		conn.commit()
	    for i in range(212,301):
		cur.execute("UPDATE METGE SET especialitat = 'Pediatra' WHERE metgeid ='M"+str(i)+"';")
		conn.commit()

	    for i in range(1,301):   
             cur.execute("SELECT DISTINCT ACCIO FROM ESDEVENIMENTS2 WHERE METGE='M"+str(i)+"'")
	     val={}
	     print "M"+str(i)+":"
             llista_accions=map(lambda x: x[0],cur.fetchall())
	     print llista_accions
             if 'Enguixar extremitat' in llista_accions:
             	especialitat="Traumatoleg"
             elif 'Electrocardiograma' in llista_accions:
             	especialitat="Cardioleg"
             else:
             	especialitat="Pediatra"
	     #print especialitat
	     print especialitat
             cur.execute("UPDATE METGE SET especialitat = '"+especialitat+"' WHERE metgeid ='M"+str(i)+"';")
             conn.commit()

             #accions_metge['M'+str(i)]={'especialitat':especialitat}
	     #print str(accions_metge)
	
	     
	    
	    print 'Num Traumatolegs'
            cur.execute("SELECT COUNT (*) FROM METGE WHERE especialitat='Traumatoleg';")
	    aux1=map(lambda x: int(x[0]),cur.fetchall())[0]


	    print 'Num Cardiolegs'
            cur.execute("SELECT COUNT (*) FROM METGE WHERE especialitat='Cardioleg';")
	    aux2=map(lambda x: int(x[0]),cur.fetchall())[0]

	    print 'Num Pediatra'
            cur.execute("SELECT COUNT (*) FROM METGE WHERE especialitat='Pediatra';")
	    aux3=map(lambda x: int(x[0]),cur.fetchall())[0]

	    percentatges=[float(aux1)/300,float(aux2)/300,float(aux3)/300]
	    print percentatges

            #import numpy as np
	    #import matplotlib.pyplot as plt
	    #from matplotlib.ticker import PercentFormatter

	    #plt.hist(percentatges, weights=np.ones(len(percentatges)) / len(percentatges))

	    #plt.gca().yaxis.set_major_formatter(PercentFormatter(1))
	    #plt.show()


	    taula2=[]
            cur.execute("SELECT DISTINCT ACCIO FROM ESDEVENIMENTS2")
            for ACCIO in map(lambda x: x[0],cur.fetchall()):
	     cur.execute("SELECT COUNT(DISTINCT  METGE)  FROM ESDEVENIMENTS2 WHERE  ACCIO='"+ACCIO+"'")
             print "L'ACCIO "+ ACCIO + ", es feta per " + str(map(lambda x: int(x[0]),cur.fetchall())[0]) + " metges diferents"
             cur.execute("SELECT COUNT(*)  FROM ESDEVENIMENTS2 WHERE  ACCIO='"+ACCIO+"'")
	     ACCIO=str(map(lambda x:int(x[0]), cur.fetchall())[0])
             print "L'ACCIO "+ ACCIO + ", s'ha fet: " + ACCIO + " cops"
	     taula2.append(float(ACCIO)*100/numesdeveniments)
	    print taula2

	    print 'Obtenim ledat dels 100 pacients que mes esdeveniments han tingut'

	
	    cur.execute("SELECT PACIENT,EDAT, COUNT (PACIENT) AS mostfrequent FROM ESDEVENIMENTS2 GROUP BY PACIENT,EDAT ORDER BY COUNT (PACIENT) DESC limit 100;")
	    pacientsordenats=cur.fetchall()
	    print pacientsordenats

            cur.execute("SELECT PACIENT,EDAT, COUNT (PACIENT) AS mostfrequent FROM ESDEVENIMENTS2 GROUP BY PACIENT,EDAT ORDER BY COUNT (PACIENT) DESC limit 100;")
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

#SELECT AVG(EDAT) FROM ESDEVENIMENT WHERE (SELECT);

#relacionar id unic amb la edat:
#SELECT DISTINCT ON (PACIENT) PACIENT,EDAT FROM ESDEVENIMENT ORDER BY PACIENT,EDAT;
#conjunt unic de edats:
#SELECT DISTINCT ON (PACIENT) EDAT FROM ESDEVENIMENT;


#creem vista (edat mitjana dels pacients de l'hospital sense repetits):
#create view my_data1 AS with data as (SELECT DISTINCT ON (PACIENT) EDAT FROM ESDEVENIMENT)
#SELECT AVG (EDAT) FROM data;
#SELECT * FROM my_data1;




#MITJA DE ESDEVENIMENT PER RANGS D'EDAT
#0-5 SELECT COUNT (*) FROM ESDEVENIMENT WHERE EDAT < 6;
#6-18 create view sisdivuit AS with dades1 as (SELECT * FROM ESDEVENIMENT WHERE EDAT > 5)
#SELECT COUNT (*) FROM dades1 WHERE EDAT < 19;
#SELECT * FROM sisdivuit;

#19-35 create view dinoutrentacinc AS with dades2 as (SELECT * FROM ESDEVENIMENT WHERE EDAT > 18)
#SELECT COUNT (*) FROM dades2 WHERE EDAT < 36;
#SELECT * FROM dinoutrentacinc;

#36-60 create view trentasisseixanta AS with dades3 as (SELECT * FROM ESDEVENIMENT WHERE EDAT > 35)
#SELECT COUNT (*) FROM dades3 WHERE EDAT < 61;
#SELECT * FROM trentasisseixanta;

#61-100 create view seixantaucent AS with dades4 as (SELECT * FROM ESDEVENIMENT WHERE EDAT > 60)
#SELECT COUNT (*) FROM dades4 WHERE EDAT < 101;
#SELECT * FROM seixantaucent;

#creat taula edats i la taula per un rang d'edat amb tots els numeros desdeveniments de tots els pacients(relacio entre PACIENT i en num de ESDEVENIMENT en el que surt)
#create view prova AS with dades as (SELECT EDAT FROM ESDEVENIMENT)SELECT * FROM dades;


# num esdeveniments per METGE relacio
#SELECT METGE, COUNT (*) FROM ESDEVENIMENT WHERE METGE IN (SELECT METGE FROM METGE) GROUP BY METGE;

#ACCIO MES REPETIDA PER RANG D'EDAT



#DROP VIEW IF EXISTS nom_taula;


