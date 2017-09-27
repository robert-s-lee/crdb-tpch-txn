# The simulator is using TPC-H data which is public source that can scale to any size required.

- TPC-H data generator https://github.com/robert-s-lee/tpch-kit

- Workload simulator https://github.com/robert-s-lee/crdb-tpch-txn

# process flow

- crdb-tpch-txn.py combines orders and lineitem into a single transaction.  

Below can be used to generate upsert, insert on conflict, or insert DMLs

```sql
crdb-tpch-txn.py -l 1000 --dml upsert 
crdb-tpch-txn.py -l 1000 --dml ioc 
crdb-tpch-txn.py -l 1000 --dml insert
```
 
- Send to data with either Java or Python.  

```sql
./crdb-tpch-txn.py -l 1000 --dml upsert | ./senddb.py
./crdb-tpch-txn.py -l 1000 --dml upsert | java -cp ~/bin/postgresql-42.0.0.jar:./ senddb
./crdb-tpch-txn.py -l 1000 --dml upsert | psql "postgres://127.0.0.1:26257/tpch?sslmode=disable&user=root"
./crdb-tpch-txn.py -l 1000 --dml upsert | go run senddb.go

```

- Example

```sql
./crdb-tpch-txn.py -l 1000 --dml upsert | more
begin;upsert into orders values (1,36901,'O',173665.47,'1996-01-02','5-LOW','Clerk#000000951',0,'nstructions sleep furiously among ');upsert into lineitem values (1,155190,7706,1,17,21168.23,0.04,0.02,'N','O','1996-03-13','1996-02-12','1996-03-22','DELIVER IN PERSON','TRUCK','egular courts above the'),(1,67310,7311,2,36,45983.16,0.09,0.06,'N','O','1996-04-12','1996-02-28','1996-04-20','TAKE BACK RETURN','MAIL','ly final dependencies: slyly bold '),(1,63700,3701,3,8,13309.60,0.10,0.02,'N','O','1996-01-29','1996-03-05','1996-01-31','TAKE BACK RETURN','REG AIR','riously. regular, express dep'),(1,2132,4633,4,28,28955.64,0.09,0.06,'N','O','1996-04-21','1996-03-30','1996-05-16','NONE','AIR','lites. fluffily even de'),(1,24027,1534,5,24,22824.48,0.10,0.04,'N','O','1996-03-30','1996-03-14','1996-04-01','NONE','FOB',' pending foxes. slyly re'),(1,15635,638,6,32,49620.16,0.07,0.02,'N','O','1996-01-30','1996-02-07','1996-02-03','DELIVER IN PERSON','MAIL','arefully slyly ex');commit;
```

