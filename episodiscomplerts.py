#!/usr/bin/python
import psycopg2 
from sets import Set
import numpy as np
import functools 
import operator
import networkx as nx
import matplotlib.pyplot as plt
import pygraphviz
'''
CREATE TABLE esdeveniments2 (ESDEVENIMENT numeric(10), EPISODI varchar(10), METGE varchar(100), PACIENT varchar(10), EDAT numeric(10), ACCIO varchar(100), DATA timestamp);
'''
def convertTuple(tup): 
		pr = functools.reduce(operator.add, (tup)) 
		return pr
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
	
			matriu=[[  2  , 0 ,  0  , 0  , 0 ,  0 , 56  ,65 , 49  , 0],
	 [  0,  77 ,  0 ,103 ,  0 ,  0  , 0 , 64 ,  0 ,  0],
	 [  0  , 0 ,  0 ,  0 , 70 , 14 ,  0 , 35 ,  0 ,  0],
	 [  0 , 99 ,  0  , 3  , 0  , 0  , 0  ,51 ,  0 ,  0],
	 [  0 ,  0 ,  0 ,  0 , 33 ,  0 ,  0 , 17  , 0 , 113],
	 [  0 ,  0 , 63 ,  0 , 14  , 0 ,  0 , 33  , 0 ,  0],
	 [ 15  , 0  , 0 ,  0 ,  0  , 0  , 0 ,  6 , 17  , 0],
	 [ 67 , 60  , 0 , 30 , 13 ,  0  , 0  ,58 , 54 ,121],
	 [ 27 ,  0   ,0  , 0  , 0  , 0  , 0 , 23 , 91  , 0],
	 [  0  , 0  , 5  , 0   ,0 ,236  , 0 ,  4 ,  0 , 10]]
			print matriu
			i=0
			j=0
			matriuResultats=np.zeros((10,10),dtype=float)
			resultats=map(lambda linea:sum(linea),matriu)
			for linea in matriu:
				for elem in linea:
					matriuResultats[i][j]=(float(elem)/resultats[i])
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
			nx.draw_networkx_nodes(G,pos,node_color='green',node_size=4450,arrowstyle='fancy')
	 
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
				nx.draw_networkx_edges(G,pos,edgelist=weighted_edges,width=width,edge_color = 'b', arrows=True, arrowstyle='fancy', arrowsize=12)
			options = {
			    'node_color': 'blue',
			    'node_size': 100,
			    'width': 3,
			    'arrowstyle': 'fancy',
			    'arrowsize': 20,
			}
			#nx.draw_networkx(G, arrows=True, **options)
			G=nx.nx_agraph.to_agraph(G)
			plt.axis('off')
			plt.show()
		except (Exception, psycopg2.DatabaseError) as error:
    #except:
	        	print(error) 

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
