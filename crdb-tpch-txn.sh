# insert each orderkey orders and lineitem as a set of txn

if [ ! -f orders.tbl ]; then 
  mkfifo orders.tbl; 
  dbgen -b ~/bin/dists.dss -T O -z -s 1000 > orders.tbl &
fi
if [ ! -f lineitem.tbl ]; then 
  mkfifo lineitem.tbl
  dbgen -b ~/bin/dists.dss -T L -z -s 1000 > lineitem.tbl &
fi

for p in 0 1;do
cockroach sql --insecure --database="tpch" -e "truncate orders;truncate lineitem" >/dev/null 2>/dev/null
(time python3 crdb-tpch-txn.py -p $p | cockroach sql --insecure --database=tpch >/dev/null 2>/dev/null) 2> returnnothing.p$p.e0.log
done

for p in 0 1;do
cockroach sql --insecure --database="tpch" -e "truncate orders;truncate lineitem" >/dev/null 2>/dev/null
(time python3 crdb-tpch-txn.py -p $p -e $'\n' | cockroach sql --insecure --database=tpch >/dev/null 2>/dev/null ) 2> returnnothing.p$p.e1.log
done

