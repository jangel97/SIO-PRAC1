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
from multiprocessing.dummy import Pool as ThreadPool

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
			#episodiiesdeveniments=map(lambda x:dict(x),episodiiesdeveniments)
			#episodiiesdeveniments=[('EP1','Radiografia'),('EP1','Caramelo'),('EP1','Radiografia'),('EP2','loko'),('EP2','loko2'),('EP2','loko3'),('EP2','loko4')]
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
			print 'loko'
			matriuResultats=np.zeros((10,10),dtype=float)
			resultats=map(lambda linea:sum(linea),matriu)
			print 'loko'
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
			print "Plot a weighted graph"
	 
			#2. Add nodes
			G = nx.DiGraph(directed=True) #Create a graph object called G
			node_list = ['Radiografia','Electrocardiograma','Mesurament i pesatge','Oximetria','Vacuna','Donar piruleta','Enguixar extremitat','Analisi de sang','Receptar medicament','Posar tireta']
			for node in node_list:
				G.add_node(node)

				#Note: You can also try a spring_layout
			pos=nx.circular_layout(G) 
			nx.draw_networkx_nodes(G,pos,node_color='yellow',node_size=4450,arrowstyle='fancy')
	 
			#3. If you want, add labels to the nodes
			labels = {}
			for node_name in node_list:
				labels[str(node_name)] =str(node_name)
			nx.draw_networkx_labels(G,pos,labels,font_size=6,arrowstyle='fancy')	
			i=0
			j=0
			for linea in matriu:
				for elem in linea:
					G.add_edge(node_list[i],node_list[j],weight=matriuResultats[i][j])
					j=j+1
				i=i+1
				j=0
		    	
			all_weights = []
			#4 a. Iterate through the graph nodes to gather all the weights
			for (node1,node2,data) in G.edges(data=True):
				all_weights.append(data['weight']) #we'll use this when determining edge thickness
			 
			#4 b. Get unique weights
			unique_weights = list(set(all_weights))
		 
		 	#4 c. Plot the edges - one by one!
			for weight in unique_weights:
			#4 d. Form a filtered list with just the weight you want to draw
				weighted_edges = [(node1,node2) for (node1,node2,edge_attr) in G.edges(data=True) if edge_attr['weight']==weight]
				#4 e. I think multiplying by [num_nodes/sum(all_weights)] makes the graphs edges look cleaner
				width = weight*len(node_list)*3.0/sum(all_weights)
				nx.draw_networkx_edges(G,pos,edgelist=weighted_edges,width=width,edge_color = 'black', arrows=True, arrowstyle='fancy', arrowsize=12)
		
			plt.axis('off')
			plt.title(filename)
			plt.savefig(filename+".png")
			plt.show()
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




