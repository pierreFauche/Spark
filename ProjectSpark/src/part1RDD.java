q1.scala
//import des classes dont nous aurons besoin
import au.com.bytecode.opencsv.CSVParser
import org.apache.spark.rdd.RDD


//nous créons un "Distributed RDD" de notre fichier CSV 
val crimeFile = "crimes.csv"
val crimeData = sc.textFile(crimeFile).cache()


//Question1:

// nous pouvons maintenant regrouper les element de la colonne «crimedesc» 
//et afficher la valeur maximum comme suit
 
   
    crimeData.mapPartitions(lines => {
      val parser=new CSVParser(',');
      lines.map(line => {
        val columns = parser.parseLine(line)
        Array(columns(5)).mkString(",")
      })
    }).countByValue().maxBy(_._2);

// Question 2
// de meme nous pouvons maintenant regrouper les element de la colonne cdatetime 
//et afficher les 3 valeurs maximum comme suit:


  
    crimeData.mapPartitions(lines => {
      val parser=new CSVParser(',');
      lines.map(line => {
        val columns = parser.parseLine(line)
        Array(columns(0).split(" ")(0)).mkString(",")         

          })
    }).countByValue().toList.sortBy(-_._2).take(3).foreach(println)


































