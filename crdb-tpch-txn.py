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
parser.add_option("-O", "--orders", dest="ordersname", default="orders.csv", help="orders csv file")
parser.add_option("--dml", dest="dml", default="insert", help="insert, upsert, ioc = insert .. on conflict update")
parser.add_option("-L", "--lineitem", dest="lineitemname", default="lineitem.csv", help="lineitem csv file")
parser.add_option("-l", "--limit", dest="orderlimit", default=100000, type=int, help="stop after processing this many orders")
parser.add_option("-b", "--batch", dest="batch", default=1000, type=int, help="commit after this many orders")
parser.add_option("-p", "--parallelize", dest="parallel", default=0, type=int, help="parallizing by adding returning nothing")
parser.add_option("-e", "--end", dest="lineend", default='', help="line terminator")
(options, args) = parser.parse_args()

ordersprocessed = 0

dml="insert"
orders_ioc=""
lineitem_ioc=""
if options.dml == "upsert":
  dml="upsert"
elif options.dml == "ioc":
  orders_ioc=" on conflict (o_orderkey) do update set o_orderkey=excluded.o_orderkey, o_custkey=excluded.o_custkey, o_orderstatus=excluded.o_orderstatus, o_totalprice=excluded.o_totalprice,o_totalprice=excluded.o_totalprice,o_orderdate=excluded.o_orderdate,o_orderpriority=excluded.o_orderpriority,o_clerk=excluded.o_clerk,o_shippriority=excluded.o_shippriority,o_comment=excluded.o_comment"
  lineitem_ioc=" on conflict (l_orderkey,l_linenumber) do update set l_orderkey=excluded.l_orderkey,l_partkey=excluded.l_partkey,l_suppkey=excluded.l_suppkey,l_linenumber=excluded.l_linenumber,l_quantity=excluded.l_quantity,l_extendedprice=excluded.l_extendedprice,l_discount=excluded.l_discount,l_tax=excluded.l_tax,l_returnflag=excluded.l_returnflag,l_linestatus=excluded.l_linestatus,l_shipdate=excluded.l_shipdate,l_commitdate=excluded.l_commitdate,l_receiptdate=excluded.l_receiptdate,l_shipinstruct=excluded.l_shipinstruct,l_shipmode=excluded.l_shipmode,l_comment=excluded.l_comment"

parallel=""
if options.parallel:
  parallel=" returning nothing"

ordersfile = open(options.ordersname,'r')
orderscsv = csv.reader(ordersfile, delimiter='\t')

lineitemfile = open(options.lineitemname,'r')
lineitemcsv = csv.reader(lineitemfile, delimiter='\t')

lineitemrow = next(lineitemcsv)
ordersrow = next(orderscsv)

while options.orderlimit > 0 and ordersprocessed < options.orderlimit and ordersrow:
  print ("begin;",end=options.lineend)
  print ("%s into orders values "% dml,end=options.lineend)
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
      + ")"
      + orders_ioc
      + parallel
      + ";"
      ,end=options.lineend)

  print ("%s into lineitem values " % dml,end=options.lineend)
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
  print (lineitem_ioc 
    + parallel 
    + ";"
    ,end=options.lineend)
  print ("commit;")
  ordersrow = next(orderscsv)
  ordersprocessed += 1

