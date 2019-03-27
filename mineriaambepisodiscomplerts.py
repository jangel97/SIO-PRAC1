#!/usr/bin/python
import psycopg2 
from sets import Set
import numpy as np
import functools 
import operator

def convertTuple(tup): 
		pr = functools.reduce(operator.add, (tup)) 
		return pr
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
            cur.execute("SELECT DISTINCT (episodiid) FROM episodi;")
	    episodiiesdeveniment=cur.fetchall()
            #episodis=map(lambda x:list(diccionari[x[0]]).append(x[1]),episodiiesdeveniment)
	    #print episodis
            #episodis=map(lambda x:x[0],episodiiesdeveniment)
	    episodiiesdeveniment=map(lambda x:(x[0]),episodiiesdeveniment)
	    #print episodiiesdeveniment
	    i=0
	    #ho limitem a 1000 esdeveniments pero ho hem de canviar
	    
	    taulaaccions=['Radiografia', 'Electrocardiograma', 'Mesurament i pesatge', 'Oximetria', 'Vacuna', 'Donar piruleta', 'Enguixar extremitat', 'Analisi de sang', 'Receptar medicament', 'Posar tireta']

	    matriu=np.zeros((10,10),dtype=int)
	    print matriu
	    for episodi in episodiiesdeveniment[:1000]:
	        #buscar tots els esdeveniments daquest mateix episodi
	    	cur.execute("SELECT esdevenimentid FROM episodi WHERE episodiid = '" + str(episodi) + "';")
		accions=[]
		#prova = convertTuple(row) 
		#print(prova)
		index=0
		for row in cur.fetchall():
			taulaaux=['taulaaccions']
			#retorna un a un els valors
			row= convertTuple(row)
			cur.execute("SELECT accio FROM esdeveniment WHERE esdevenimentid = " + str(row) + ";")
			accio=map(lambda x:x[0] ,cur.fetchall())[0]
			accions.append(accio)
		if len(diccionari)>1:
			#print diccionari
			i=0
			for accio in accions:
				print accio
				index1=taulaaccions.index(accio)
				try:
					index2=taulaaccions.index(accions[i+1])
					matriu[index1][index2]=matriu[index1][index2]+1	
					i=i+1
				except:
					print "error brutal"
	    	diccionari[episodi]=accions
	    print matriu

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
