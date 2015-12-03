// creation Context
val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

//creation cast 
case class Cast(cdatetime:String,address:String,district:Int,beat:String,grid:Int,crimedescr:String,ucr_ncic_code:Int,latitude:Float,longitude:Float)

//nous créons un "Distributed RDD" de notre fichier CSV 
val file = sc.textFile("crimes.csv").
mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else
iter }

// NB ici nous splitons la colonne cdatetime pour nous debarasser de l'heure dans la date
val casts = file.map(line => {
val l = line.split(",")
Cast(l(0).split(" ")(0),l(1), l(2).toInt, l(3),l(4).toInt,l(5),l(6).toInt,l(7).toFloat,l(8).toFloat)
})


//on cast en DataFrame
val castsDF = casts.toDF



 //Question1:

	castsDF.groupBy('crimedescr).count().orderBy(desc("count")).show(1)

 //Question2:

 	 castsDF.groupBy('cdatetime).count().orderBy('count.desc).show(3)
 
 //Question3:
	    
	 val nbCrimes = castsDF.groupBy('crimedescr).count().orderBy(desc("count"))
	 nbCrimes.select(nbCrimes("crimedescr"), nbCrimes("count")/(31)).show()
