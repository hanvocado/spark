package log.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class FilterIP {

	public static void main(String[] args) {
		if (args.length < 2) {
			System.out.println("Please add <input file> <output dir>");
			System.exit(-1);
		}
		
		String inputPath = args[0];
		String outputDir = args[1];
		
		SparkSession spark = SparkSession.builder()
				.appName("IPs request google")
				.getOrCreate();
		
		Dataset<Row> data = spark.read().text(inputPath);
		Dataset<Row> filteredData = data.filter(data.col("value").contains("www.google.com"));
		Dataset<Row> result = filteredData.select(functions.regexp_extract(
				functions.col("value"), "^(\\S+)", 1).alias("ip")
				).distinct();
		result.show(5);
		result.write().text(outputDir);
		System.out.println("Completed");
		spark.stop();
	}

}
