package sql

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext


object sparkSQL {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SparkSQL")
  
    val sqlC= new SQLContext(sc)
//Read de json adn put it in input
    val input = sqlC.read.json("zips.json")
    input.show(10)
    
    //Filtro la poblacion de mas de 10000 y seleccionamos los cp
    val cpPoblacionGrande =input.select("_id").filter("pop>10000")
    cpPoblacionGrande.show(10)
    
    input.createOrReplaceTempView("input")
   // input.as("input")
    //Lo mismo que la consulta anterior pero con sentencia sql
    val cpPoblacionSQL=sqlC.sql("Select _id from input where pop>10000")
    cpPoblacionSQL.show(10)
    
    //Sacamos las ciudades con mas de 100 CP
    sqlC.sql("Select city from input group by city having count(_id)>100").show()
    
    //Poblacion de wisconsin
    sqlC.sql("Select  sum(pop) as poblacion from input where state='WI'").show()
    
    //Los 5 estados con mayor poblacion
    sqlC.sql("Select state,sum(pop) from input group by state order by 2 desc ").show(5)
    
    
  }
    
}
  