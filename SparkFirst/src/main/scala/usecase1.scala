package SparkBatch
import org.apache.spark.sql.SparkSession
object usecase1 {
   def main(args: Array[String]):Unit={
     var inputpath=args(0)  //H:\\spark_data\\olympix.csv
     var outputpath=args(1) //H:\\result37\\res_agg.csv
     System.setProperty("hadoop.home.dir", "C:/HADOOP/winutils/");
   val spark = SparkSession.builder.master("local").appName("HariKrishna").getOrCreate() 
      var readcsvDF=spark.read.option("header",true).csv(inputpath)
    var aggDF=readcsvDF.groupBy("country").count()
    aggDF.coalesce(1).write.mode("overWrite").format("csv").save(outputpath)
   }
}