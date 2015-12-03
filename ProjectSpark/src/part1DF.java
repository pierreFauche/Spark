import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.rdd.RDD

val crimeFile = "crimes.csv"
val crimeData = sc.textFile(crimeFile).cache()





crimeData.mapPartitions(lines => {
         val parser = new CSVParser(',')
         lines.map(line => {
           parser.parseLine(line).mkString(",")
         })
       }).take(5).foreach(println)

def dropHeader(data: RDD[String]): RDD[String] = {
         data.mapPartitionsWithIndex((idx, lines) => {
           if (idx == 0) {
             lines.drop(1)
           }
           lines
         })
       }

val withoutHeader: RDD[String] = dropHeader(crimeData)

withoutHeader.mapPartitions(lines => {
         val parser = new CSVParser(',')
         lines.map(line => {
           parser.parseLine(line).mkString(",")
         })
       }).take(5).foreach(println)


/////////////////////////////////////Ma reponse question 1
    crimeData.mapPartitions(lines => {
      val parser=new CSVParser(',');
      lines.map(line => {
        val columns = parser.parseLine(line)
        Array(columns(5)).mkString(",")
      })
    }).countByValue().maxBy(_._2)._1;

//////////////////////////////////////// Ma reponse question 2


  
    crimeData.mapPartitions(lines => {
      val parser=new CSVParser(',');
      lines.map(line => {
        val columns = parser.parseLine(line)
        Array(columns(0).split(" ")(0)).mkString(",")         

          })
    }).countByValue().toList.sortBy(-_._2).take(3).foreach(println)
