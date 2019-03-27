#!/usr/bin/python
import psycopg2 
from sets import Set
import numpy as np


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

            print 'Mineria'
            print 'Juntar episodis amb esdeveniments'
	    diccionari={}
            cur.execute("SELECT * FROM episodi;")
	    episodiiesdeveniment=cur.fetchall()
            #episodis=map(lambda x:list(diccionari[x[0]]).append(x[1]),episodiiesdeveniment)
	    #print episodis
            #episodis=map(lambda x:x[0],episodiiesdeveniment)
	    episodiiesdeveniment=map(lambda x:(x[0],x[1]),episodiiesdeveniment)
	    #print episodiiesdeveniment
	    i=0
	    #ho limitem a 1000 esdeveniments pero ho hem de canviar
	   
	    taulaaccions=['Radiografia', 'Electrocardiograma', 'Mesurament i pesatge', 'Oximetria', 'Vacuna', 'Donar piruleta', 'Enguixar extremitat', 'Analisi de sang', 'Receptar medicament', 'Posar tireta']

	    matriu=np.zeros((10,10),dtype=int)
	    print matriu
	    for episodi in episodiiesdeveniment[:1000]:
	        lst=[k for k in episodiiesdeveniment if episodi[0] in k]
	   	diccionari[episodi[0]]=map(lambda x:x[1],lst)
		esdeveniments= map(lambda x:x[1],lst)
		accions=[]
		#print esdeveniments
	   	for esdeveniment in esdeveniments:	
			cur.execute("SELECT accio FROM esdeveniment WHERE esdevenimentid = '" + str(esdeveniment) + "';")
		    	accio=map(lambda x:x[0] ,cur.fetchall())[0]
			accions.append(accio)
			#print accions	   
		if len(accions)>1:
			print accions
			i=0
			print accions
			for accio in accions:
				print accio
				index1=taulaaccions.index(accio)
				try:
					index2=taulaaccions.index(accions[i+1])
					matriu[index1][index2]=matriu[index1][index2]+1	
					i=i+1
				except:
					print "error brutal"
				#print matriu([index1][index2])
	    print matriu
				
			
	    #print diccionari
	    
	    '''
	    print 'Esdeveniment inici'
	    accionsinici=[]
	    accionsinicinorepetits = {}
	    esdevenimentsinici = map(lambda x:x[0], diccionari.values())
	    for esdeveniment in esdevenimentsinici:	
		cur.execute("SELECT accio FROM esdeveniment WHERE esdevenimentid = '" + str(esdeveniment) + "';")
	    	episodiiesdeveniment=map(lambda x:x[0] ,cur.fetchall())[0]
	   	accionsinici.append(episodiiesdeveniment)
		
	    accionsinicinorepetits=list(set(accionsinici)) 
	    print accionsinicinorepetits
	    
	    print 'Esdeveniments final'
	    accionsfinal=[]
	    accionsfinalnorepetits={}
	    esdevenimentsfinal=map(lambda x:x[-1], filter(lambda x:len(x)>1,diccionari.values()))
	    for esdevenimentfinal in esdevenimentsfinal:
		cur.execute("SELECT accio FROM esdeveniment WHERE esdevenimentid = '" + str(esdevenimentfinal) + "';")
		episodiiesdevenimentfinal=map(lambda x:x[0] ,cur.fetchall())[0]
		accionsfinal.append(episodiiesdevenimentfinal)
	    print 'Llista accions finals'
	    print accionsfinal
	    accionsfinalnorepetits=list(set(accionsfinal))
	    print 'Llista accions finals no repetits'
	    print accionsfinalnorepetits
	    '''

	    
	    
 
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