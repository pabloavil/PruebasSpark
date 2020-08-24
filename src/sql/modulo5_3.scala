package sql

import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.parquet.format.StringType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row


object modulo5_3 {
  
    def main(args: Array[String]) {
      
        // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "SparkSQL")
    
      

        var ssc = new org.apache.spark.sql.SQLContext(sc) 
        
        
        var ruta_datos="DataSetPartidos.txt" 
        
        
        var datos =sc.textFile(ruta_datos) 
        
        
        val schemaString = "idPartido::temporada::jornada::EquipoLocal::EquipoVisitante::golesLocal::golesVisitante::fecha::timestamp" 
        
        val schema =  StructType(schemaString.split("::").map(fieldName => StructField(fieldName,org.apache.spark.sql.types.StringType, true))) 
        
        
        val rowRDD = datos.map(_.split("::")).map(p => Row(p(0), p(1) , p(2) , p(3) , p(4) , p(5) , p(6) , p(7) , p(8).trim)) 
        
        
        val partidosDataFrame = ssc.createDataFrame(rowRDD, schema) 
        
        partidosDataFrame.registerTempTable("partidos")
        
        ssc.sql("select * from partidos").show(10)
        
        ssc.sql("select EquipoVisitante ,temporada, sum(golesVisitante) from partidos where EquipoVisitante like '%Oviedo%' group by temporada ,EquipoVisitante order by 3 desc ").show(1)
        
        ssc.sql("select count(distinct(temporada)) as temporadasOviedo from partidos where EquipoVisitante like '%Oviedo%' or EquipoLocal like '%Oviedo%'").show(1)
        ssc.sql("select count(distinct(temporada)) as temporadasGijon from partidos where EquipoVisitante like '%Gijon%' or EquipoLocal like '%Gijon%'").show(1)   
        
        
    }
    
  
}