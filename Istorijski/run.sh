# Moram rucnu komandu : docker exec -it namenode bash i posle toga hadoop fs -copyFromLocal /asvsp/data/WeatherEvents_Jan2016-Dec2022.csv /weather 
#(ali pre ovoga rucno na HDFS-u napraviti weather folder)

# Ova radi - pokretanje ddl skripte radi (otkomentarisati, preimenovati u run.bat i onda pokrenuti)
#docker exec -it hive-server bash -c "beeline -u jdbc:hive2://localhost:10000 -f ./hive/scripts/asvsp/HiveSkripta.sql"

#docker exec -it spark-master bash -c "./spark/bin/spark-submit ./asvsp/scripts/pre-process.py"

# KONEKCIONI STRING u supersetu glasi: hive://hive-server:10000, a ne jdbc:hive2://localhost:10000 tj. ne ovo jdbc:hive2://hive-server:10000