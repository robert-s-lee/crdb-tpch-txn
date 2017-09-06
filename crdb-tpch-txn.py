import csv
from optparse import OptionParser

def ifnull(val,ischar=False):
    if val == "\\N":
        return "NULL"
    else:
        if ischar:
            return "'" + val + "'"
        else:
            return val

parser = OptionParser()
parser.add_option("-O", "--orders", dest="ordersname", default="orders.tbl", help="orders csv file")
parser.add_option("-L", "--lineitem", dest="lineitemname", default="lineitem.tbl", help="lineitem csv file")
parser.add_option("-l", "--limit", dest="limit", default=10000, help="stop after processing this many orders")
parser.add_option("-l", "--batch", dest="batch", default=10000, help="commit after this many orders")
parser.add_option("-p", "--parallelize", dest="parallel", default=1, type=int, help="parallizing by adding returning nothing")
parser.add_option("-e", "--end", dest="lineend", default='', help="line terminator")
(options, args) = parser.parse_args()

ordersprocessed = 0

parallel=""
if options.parallel != 0:
  parallel=" returning nothing"

ordersfile = open(options.ordersname,'r')
orderscsv = csv.reader(ordersfile, delimiter='\t')

lineitemfile = open(options.lineitemname,'r')
lineitemcsv = csv.reader(lineitemfile, delimiter='\t')

lineitemrow = next(lineitemcsv)
ordersrow = next(orderscsv)

print ("begin;",end=options.lineend)
while options.orderlimit > 0 and ordersprocessed < options.orderlimit and ordersrow:
  print ("insert into orders values ",end=options.lineend)
  print ("("
      + ifnull(ordersrow[0]) # o_orderkey           INTEGER NOT NULL,
      + "," + ifnull(ordersrow[1])          # o_custkey            INTEGER NOT NULL,
      + "," + ifnull(ordersrow[2],ischar=True) # o_orderstatus        CHAR(1) NOT NULL,
      + "," + ifnull(ordersrow[3])          # o_totalprice         DECIMAL(15,2) NOT NULL,
      + "," + ifnull(ordersrow[4],ischar=True) # o_orderdate          DATE NOT NULL,
      + "," + ifnull(ordersrow[5],ischar=True) # o_orderpriority      CHAR(15) NOT NULL,
      + "," + ifnull(ordersrow[6],ischar=True) # o_clerk              CHAR(15) NOT NULL,
      + "," + ifnull(ordersrow[7])          # o_shippriority       INTEGER NOT NULL,
      + "," + ifnull(ordersrow[8],ischar=True) # o_comment            VARCHAR(79) NOT NULL,
      + ")" + parallel
      + ";"
      ,end=options.lineend)

  print ("insert into lineitem values ",end=options.lineend)
  prefix = ""
  while lineitemrow[0] == ordersrow[0]: 
    print (prefix + "("
          + ifnull(lineitemrow[0]) # l_orderkey      INTEGER NOT NULL,
          + "," + ifnull(lineitemrow[1]) # l_partkey       INTEGER NOT NULL,
          + "," + ifnull(lineitemrow[2]) # l_suppkey       INTEGER NOT NULL,
          + "," + ifnull(lineitemrow[3]) # l_linenumber    INTEGER NOT NULL,
          + "," + ifnull(lineitemrow[4]) # l_quantity      DECIMAL(15,2) NOT NULL,
          + "," + ifnull(lineitemrow[5]) # l_extendedprice DECIMAL(15,2) NOT NULL,
          + "," + ifnull(lineitemrow[6]) # l_discount      DECIMAL(15,2) NOT NULL,
          + "," + ifnull(lineitemrow[7]) # l_tax           DECIMAL(15,2) NOT NULL,
          + "," + ifnull(lineitemrow[8],ischar=True) # l_returnflag    CHAR(1) NOT NULL,
          + "," + ifnull(lineitemrow[9],ischar=True) # l_linestatus    CHAR(1) NOT NULL,
          + "," + ifnull(lineitemrow[10],ischar=True) # l_shipdate      DATE NOT NULL,
          + "," + ifnull(lineitemrow[11],ischar=True) # l_commitdate    DATE NOT NULL,
          + "," + ifnull(lineitemrow[12],ischar=True) # l_receiptdate   DATE NOT NULL,
          + "," + ifnull(lineitemrow[13],ischar=True) # l_shipinstruct  CHAR(25) NOT NULL,
          + "," + ifnull(lineitemrow[14],ischar=True) # l_shipmode      CHAR(10) NOT NULL,
          + "," + ifnull(lineitemrow[15],ischar=True) # l_comment       VARCHAR(44) NOT NULL,
          + ")" 
          ,end=options.lineend)
    lineitemrow = next(lineitemcsv)
    prefix = ","
  print (parallel + ";",end=options.lineend)
  print ("commit;")
  ordersrow = next(orderscsv)
  ordersprocessed += 1

