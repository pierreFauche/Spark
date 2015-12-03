PARTIE1:

	RDD:Le code de cette partie se trouve 
	
		1) voici le resultat obtenu:
			(10851(A)VC TAKE VEH W/O OWNER,653) 
		2) voici les resultat obtenu:	
			(1/11/06,311)
			(1/18/06,295)
			(1/17/06,282)
		3) ...

	DATA FRAME:Le code de cette partie ce trouve:

		1) voici le resultat obtenu:
				+--------------------+-----+                                         
				|          crimedescr|count|
				+--------------------+-----+
				|10851(A)VC TAKE V...|  653|
				+--------------------+-----+
		2)  voici le resultat obtenu:
				+---------+-----+                                                    
				|cdatetime|count|
				+---------+-----+
				|  1/11/06|  311|
				|  1/18/06|  295|
				|  1/17/06|  282|
				+---------+-----+

		3) voici le resultat obtenu:
				+--------------------+------------------+   
				|          crimedescr|      (count / 31)|
				+--------------------+------------------+
				|10851(A)VC TAKE V...| 21.06451612903226|
				|TOWED/STORED VEH-...|14.935483870967742|
				|459 PC  BURGLARY ...|14.903225806451612|
				|TOWED/STORED VEHICLE|              14.0|
				|459 PC  BURGLARY ...|11.483870967741936|
				|      MISSING PERSON|  8.64516129032258|
				|10851 VC AUTO THE...| 7.193548387096774|
				|594(B)(2)(A) VAND...| 6.290322580645161|
				|TRAFFIC-ACCIDENT ...| 5.870967741935484|
				|TRAFFIC-ACCIDENT-...| 5.645161290322581|
				|459 PC  BURGLARY ...| 4.354838709677419|
				|484 PETTY THEFT/L...| 3.838709677419355|
				|594(B)(1)PC  VAND...| 3.774193548387097|
				|     CASUALTY REPORT|3.7419354838709675|
				|5150 WI DANGER SE...|3.4193548387096775|
				|    20002(A) HIT/RUN|3.3225806451612905|
				|11364 HS POSS DRU...| 3.193548387096774|
				|484 PC   PETTY TH...| 3.193548387096774|
				|211 PC  ROBBERY U...| 3.193548387096774|
				|NON INJ HR/MAIL O...| 2.903225806451613|
				+--------------------+------------------+
				
 PARTIE 2:

 		1)ok
 		2)ok


./bin/spark-shell --packages com.databricks:spark-csv_2.11:1.2.0 
castsDF.groupBy('crimedescr).count().orderBy(desc("count")).repartition(1).write.format("com.databricks.spark.csv").save("monCSV2.csv")
