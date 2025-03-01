import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Main {

	public static void main(String[] args) {
		SparkSession spark = SparkSession.builder().appName("testSpark").master("local").getOrCreate();
		String logFile = "E:/Spark/spark-3.5.3-bin-hadoop3/README.md";
		Dataset<String> logData = spark.read().textFile(logFile).cache();
		
		long numAs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("a")).count();
		long numBs = logData.filter((org.apache.spark.api.java.function.FilterFunction<String>)s -> s.contains("b")).count();

	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
		spark.stop();
	}

}
