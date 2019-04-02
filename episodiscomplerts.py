#!/usr/bin/python
import psycopg2 
from sets import Set
import numpy as np
import functools 
import operator
import networkx as nx
import matplotlib.pyplot as plt
import pygraphviz
from itertools import izip
import pandas as pd


def generateGraph(consulta,filename):
	try:
       			conn = psycopg2.connect("host=localhost dbname=siodefinitiva user=postgres password=postgres")

			cur = conn.cursor()
	
			print 'Mineria'
			print 'Juntar episodis amb esdeveniments'
			   
			taulaaccions=['Radiografia', 'Electrocardiograma', 'Mesurament i pesatge', 'Oximetria', 'Vacuna', 'Donar piruleta', 'Enguixar extremitat', 'Analisi de sang', 'Receptar medicament', 'Posar tireta']
			matriu=np.zeros((10,10),dtype=int)
			print matriu
			diccionari={}
			cur.execute(consulta)
			episodiiesdeveniments=cur.fetchall()	
			diccionari={}
			for acciioesdev in episodiiesdeveniments:
				diccionari.setdefault(acciioesdev[0], []).append(acciioesdev[1])


			for episodi in diccionari.keys():
				accions=diccionari[str(episodi)]
				for accio1,accio2 in zip(accions, accions[1 : ]):
					index1=taulaaccions.index(accio1)
					index2=taulaaccions.index(accio2)
					matriu[index1][index2]=matriu[index1][index2]+1
			print matriu
								
			i=0
			j=0

			matriuResultats=np.zeros((10,10),dtype=float)
			resultats=map(lambda linea:sum(linea),matriu)

			for linea in matriu:
				for elem in linea:
					if resultats[i]!=0:
						matriuResultats[i][j]=(float(elem)/resultats[i])
					else:
						matriuResultats[i][j]=0
					j=j+1
				i=i+1
				j=0
		
			print matriuResultats

	 		import igraph
			conn_indices = np.where(matriuResultats)
			weights = matriuResultats[conn_indices]
			edges = zip(*conn_indices)
			G = igraph.Graph(edges=edges, directed=True)
			G.vs['label'] = taulaaccions
			G.es['weight'] = weights
			G.es['width'] = weights
			layout = G.layout("circle")
			visual_style = dict()
			visual_style["vertex_size"] = 20
			visual_style["vertex_label_size"] = 20
			visual_style["vertex_label_dist"] = 2
			visual_style["vertex_color"] = "white"
			visual_style["vertex_label_color"] = "blue"
			visual_style["layout"] = layout
			visual_style["bbox"] = (1500, 1500)
			visual_style["margin"] = 100
			visual_style["edge_label"] = weights
			out=igraph.plot(G,**visual_style)
			out.save(filename)


			return matriu

			cur.close()
			
	except (Exception, psycopg2.DatabaseError) as error:
    #except:
	        print(error)
	finally:
	        if conn is not None:
	            conn.close()
	            print('Database connection closed.')
	return None

if __name__ == '__main__':
	import sys
	print generateGraph(str(sys.argv[1]), str(sys.argv[2]))
	#generateGraph("SELECT EPISODI,ACCIO FROM esdeveniments2 WHERE EPISODI IN (SELECT DISTINCT(EPISODI) FROM ESDEVENIMENTS2) GROUP BY EPISODI,ACCIO;")
	#generateGraph("SELECT episodi,accio from esdeveniments2 where metge IN (select metgeid from metge  where especialitat='Traumatoleg') GROUP BY episodi,accio;")
	#generateGraph("SELECT episodi,accio from esdeveniments2 where metge IN (select metgeid from metge  where especialitat='Cardioleg') GROUP BY episodi,accio;")
	#generateGraph("SELECT episodi,accio from esdeveniments2 where metge IN (select metgeid from metge  where especialitat='Pediatra') GROUP BY episodi,accio;")




