package pr1
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.streaming.Time


object estadisticas {
 
  case class Padron(COD_BARRIO:Integer, COD_DISTRITO :Integer , COD_DIST_BARRIO:Integer , COD_DIST_SECCION:Integer,
      COD_EDAD_INT:Integer, COD_SECCION:Integer, DESC_BARRIO:String
      , DESC_DISTRITO : String , EspanolesHombres:Integer, EspanolesMujeres:Integer, ExtranjerosHombres:Integer, ExtranjerosMujeres:Integer) {
 
  def numberOfPeople() : Int = {
    return EspanolesHombres + EspanolesMujeres + ExtranjerosHombres + ExtranjerosMujeres;
  }
 
}
  
  
    def main(args: Array[String]) {
      
     // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SparkSQL")
    val sqlSC = new SQLContext(sc)
    
    val spark = SparkSession.builder().enableHiveSupport().getOrCreate()
    import spark.implicits._
    
    //NO HE CONSEGUIDO QUITAR LOS WHITESPACES EN LA CARGA
    val data = sqlSC.read.format("csv").option("ignoreTrailingWhiteSpace",true).option("quotes", "\"").option("inferSchema","true")
                        .option("delimiter",";").option("header", true)
                        .load("Rango_Edades_Seccion_202007.csv")
      
                       
                       
                        
              
   data.createOrReplaceTempView("padron")
   
    //Suma de españoles hombre por dsistrito
    //sqlSC.sql("SELECT desc_distrito , sum(espanoleshombres) as total  FROM padron GROUP BY desc_distrito  order by (2)").show(30);     
  

//PARA LA 1ª FORMA
      //agrupamos por clave distrito (tiene que haber una forma mas sencilla de renombrar directamente en la aggregacion) que con //.withColumnRenamed("sum(espanoleshombres)", "espanoleshombres")
      val padronDistrito= data.groupBy("DESC_DISTRITO").agg(sum ("espanoleshombres").as("espanoleshombres" ))
      //val padronDistrito2=data.reduce(func)//USAR REDUCE Y SUMAR LOS ESPAÑLOLES??
      padronDistrito.createOrReplaceTempView("padronDistrito")
      spark.time(
         // ---------  Time taken: 4084 ms
        sqlSC.sql("SELECT avg(espanoleshombres) as avg,max(espanoleshombres) as max,min(espanoleshombres) as min"+
                   ",stddev(espanoleshombres) as stddev  FROM 	padronDistrito").show()
                 )
     
//LA SEGUNDA FORMA REQUIERE spark.implicits_
    
      val padronDistRDD=data.groupBy("desc_distrito").sum("espanoleshombres").map{row=>(row(0).asInstanceOf[String].trim(),row(1).asInstanceOf[Long])}.rdd
      //padronDistRDD.take(10).foreach(println)
       spark.time(
           //--------------  Time taken: 1161 ms
            println(padronDistRDD.map(_.swap).max().swap +" es el distrito con la mayor poblacion de hombres españoles ")
       )

//LA TERCERA FORMA       
      //Se puede conseguir con una sola consulta usando una tabla derivda que agrupe por distritos

         val mediaEspañoles=sqlSC.sql("Select avg(esp) as medHombEspDist , max(esp) as maxHombEspDist ,  "+
             " min(esp) as minHombEspDist  , stddev(esp)  as desviacionEstandarHombEspDist  " + 
             " from (SELECT sum(espanoleshombres) as esp FROM padron group BY desc_distrito )")
             
      spark.time(
          //-----------Time taken: 2108 ms
         mediaEspañoles.show()
       )

        val mediaPersonas=sqlSC.sql("Select avg(per) as medPerDist , max(per) as maxPerist ,  "+
           " min(per) as minPerDist  , stddev(per)  as desviacionEstandarPerDist  " + 
           " from (SELECT sum(espanoleshombres)+sum(espanolesmujeres)+sum(extranjeroshombres)+sum(extranjerosmujeres) as per FROM padron group BY desc_distrito  )")
       mediaPersonas.show()
       
       
 //-------------------DATASETS--------------------------------
      
       val dataDS = data.as[Padron]
       val distritoDS = dataDS.groupByKey(_.DESC_DISTRITO).agg(sum($"EspanolesHombres").alias("sumEspañoles").as[Integer])
  spark.time(
      
      //------------Time taken: 2360 ms
       distritoDS.agg(avg($"sumEspañoles").alias("Media Españoles"),
           max($"sumEspañoles").alias("Maximo Españoles"),
           min($"sumEspañoles").alias("Minimo Españoles"),
           stddev($"sumEspañoles").alias("Desviacion tipica Españoles"))
           .show()
          
     )

    }  
}