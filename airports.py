from gremlin_python import statics
from gremlin_python.structure.graph import Graph
from gremlin_python.process.graph_traversal import __
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection
from itertools import islice

#starting connection
graph=Graph()
connection=DriverRemoteConnection('ws://localhost:8182/gremlin', 'g')
g=graph.traversal().withRemote(connection)

print "Start: Drop all vertices"
g.V().drop().iterate()
print "Finish: Drop all vertices"

filename="airports.dat"

number_of_lines=7698#define the number of lines to read (MAX: 7698)

vertices={}

#row[0] - ID //// row[1] - name //// row[3] - country
with open(filename,'r') as input_file:
    lines_cache=islice(input_file, number_of_lines)
    print "Next",number_of_lines,"batch"
    for current_line in lines_cache:
        row=current_line.split(',')

        name=row[1]
        name=name[slice(1,len(name)-1)]#removing quation marks
        country=row[3]
        country=country[slice(1,len(country)-1)]#removing quation marks
        iata=row[4]
        iata=iata[slice(1,len(iata)-1)]#removing quation marks

        print "Add airport:",name
        vertices[iata]=g.addV('airport').property('ID',row[0]).property('name',name).property('country',country).property('IATA',iata).next()
        #adding vertices

filename="routes.dat"

number_of_lines=67658#define the number of lines to read (MAX: 67658)

with open(filename,'r') as input_file:
    lines_cache=islice(input_file, number_of_lines)
    print "Next",number_of_lines,"batch"
    for current_line in lines_cache:
        row=current_line.split(',')

        print "Add route from",row[2],'to',row[4]#row[2] - source //// row[4] - destination

        try:
            g.V(vertices[row[2]]).addE('route').to(vertices[row[4]]).iterate()
        except KeyError:
            print 'Warrning! KeyError:',row[2],'or KeyError:',row[4]

#closing connection
connection.close()
