import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark._
import org.apache.spark.sql.api.java._
//import org.apache.spark.sql.api.java.StructField
object SparkQueries {
	def main {
		val conf = new SparkConf()//.setAppName(appName).setMaster(master)
		val sc = new SparkContext(conf)
		val sqlContext = new org.apache.spark.sql.SQLContext(sc)
		val artist = sc.textFile("/user/hduser/artist")
		val schemaString = "id name gid begindate enddate type"
		import org.apache.spark.sql.Row;
		//import org.apache.spark.sql.types._//{StructType, StructField, StringType};
		val schema = StructType(schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))
		val rowRDD = artist.map(_.split(",")).map(a => Row(a(0).trim,a(1).trim, a(2).trim, a(7).trim, a(8).trim, a(9).trim))
		val artistDF = sqlContext.createDataFrame(rowRDD, schema)
		artistDF.registerTempTable("artistt")
		val allrecords = sqlContext.sql("SELECT * FROM artistt")
		allrecords.show()
		val bornInThe90s = sqlContext.sql("SELECT * FROM artistt WHERE type='1' AND begindate LIKE('199%')")
		bornInThe90s.show()
	}
}