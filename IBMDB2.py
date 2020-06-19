import ibm_db
from itertools import islice

#starting connection
conn = ibm_db.connect("database","user","password")

#DELETE
#delete = "DELETE FROM airports"
#stmt = ibm_db.exec_immediate(conn,delete)
#delete = "DELETE FROM routes"
#stmt = ibm_db.exec_immediate(conn,delete)

filename="airports.dat"

number_of_lines = 7698#define the number of lines to read (MAX: 7698)

vertices = {}

#row[0] - ID //// row[1] - name //// row[3] - country
with open(filename, 'r') as input_file:
    lines_cache = islice(input_file, number_of_lines)
    print "Next ", number_of_lines, " batch"
    for current_line in lines_cache:
        row = current_line.split(',')

        name=row[1]
        name=name[slice(1,len(name)-1)]#removing quation marks
        country=row[3]
        country=country[slice(1,len(country)-1)]#removing quation marks
        if(len(row[4])==2 or len(row[4])==5):
            iata=row[4]
        else:
            iata=row[5]

        iata=iata[slice(1,len(iata)-1)]#removing quation marks


        #print row[0],name,country,iata
        name=name.replace("'","")
        country=country.replace("'","")
        #vertices[row[4]]="INSERT INTO airports values("+"1"+row[0]+",'"+"CP1"+name+"','"+country+"','"+"CP1"+iata+"');"
        #stmt = ibm_db.exec_immediate(conn,unicode(vertices[row[4]],"utf-8"))

filename="routes.dat"

number_of_lines = 67658#define the number of lines to read (MAX: 67658)

with open(filename, 'r') as input_file:
    lines_cache = islice(input_file, number_of_lines)
    print "Next ", number_of_lines, " batch"
    for current_line in lines_cache:
        row = current_line.split(',')

        #print "Add route from",row[2],'to',row[4]#row[2] - source //// row[4] - destination

        try:
            #sql = "INSERT INTO routes values('"+row[2]+"','"+row[4]+"');"
            #stmt = ibm_db.exec_immediate(conn,sql)
        except KeyError:
            print 'Warrning! KeyError:',row[2],'or KeyError:',row[4]
