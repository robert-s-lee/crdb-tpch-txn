# insert each orderkey orders and lineitem as a set of txn

if [ ! -f orders.tbl ]; then 
  mkfifo orders.tbl; 
  dbgen -b ~/bin/dists.dss -T O -z -s 1000 > orders.tbl &
fi
if [ ! -f lineitem.tbl ]; then 
  mkfifo lineitem.tbl
  dbgen -b ~/bin/dists.dss -T L -z -s 1000 > lineitem.tbl &
fi

cockroach sql --insecure <<EOF
set sql_safe_updates = false;
EOF

for p in 0 1;do
cockroach sql --insecure --database="tpch" -e "truncate orders;truncate lineitem" >/dev/null 2>/dev/null
(time python3 crdb-tpch-txn.py -p $p | cockroach sql --insecure --database=tpch >/dev/null 2>/dev/null) 2> returnnothing.p$p.e0.log
done

for p in 0 1;do
cockroach sql --insecure --database="tpch" -e "truncate orders;truncate lineitem" >/dev/null 2>/dev/null
(time python3 crdb-tpch-txn.py -p $p -e $'\n' | cockroach sql --insecure --database=tpch >/dev/null 2>/dev/null ) 2> returnnothing.p$p.e1.log
done

cat >> ~/.psqlrc <<EOF
\timing on
EOF

time python3 crdb-tpch-txn.py -l 1000 --upsert 1 |  psql "port=26257 user=root host=127.1 dbname=tpch_il"  > upsert_il.log 2>&1

time python3 crdb-tpch-txn.py -l 1000 --dml upsert |  psql "port=26257 user=root host=127.1 dbname=tpch_nil"  > upsert_nil.log 2>&1
time python3 crdb-tpch-txn.py -l 1000 --upsert ioc |  psql "port=26257 user=root host=127.1 dbname=tpch_nil"  > ioc_nil.log 2>&1

# 2 17 20 21 22 
for q in 1 3 4 5 6 7 8 9 10 11 12 13 14 15 16 18 19; do echo $q; 
  (time qgen $q  |  psql "port=26257 user=root host=127.1 dbname=tpch_il" >   /dev/null) 2>&1 | paste - - - - -
done

tail -n +2 upsert_il.log | paste - - - - - - - - | awk '{print "il " $0}' > upsert.csv
tail -n +2 upsert_nil.log | paste - - - - - - - - | awk '{print "nil " $0}' >> upsert.csv



