val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._


case class Cast(cdatetime:String,address:String,district:Int,beat:String,grid:Int,crimedescr:String,ucr_ncic_code:Int,latitude:Float,longitude:Float)

val file = sc.textFile("crimes.csv").
mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else
iter }

val casts = file.map(line => {
val l = line.split(",")
Cast(l(0).split(" ")(0),l(1), l(2).toInt, l(3),l(4).toInt,l(5),l(6).toInt,l(7).toFloat,l(8).toFloat)
})

val castsDF = casts.toDF

castsDF.show


 //1

	 //le bon
	 castsDF.groupBy('crimedescr).count().orderBy(desc("count")).show(1)
	 castsDF.groupBy('ucr_ncic_code).count().orderBy(desc("count")).show(1)

 //2

 	 castsDF.groupBy('cdatetime).count().orderBy('count.desc).show(3)
 //3
	    
	 val nbCrimes = castsDF.groupBy('crimedescr).count().orderBy(desc("count"))
	 nbCrimes.select(nbCrimes("crimedescr"), nbCrimes("count")/(31)).show()