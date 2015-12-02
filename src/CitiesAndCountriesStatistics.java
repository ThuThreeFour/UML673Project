import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/* map(<key, value>){ // data preparation
 *  // filter out field of interests
 *  // a place to drop bad records
 * 	outputs <key, value>
 * 	(Lowell, 5000)
 *  (Boston, 10000)
 * }  
 * */

/* MapReduce framework sorts and group the kay-value pairs by keys then feed that to reduce function
 * */

/* reduce(<key, value>){ ret <key, value>}
 * // iterate throught the list and pick out value based on condition 
 * */

public class CitiesAndCountriesStatistics {

	// Entry point
	public static void main(String[] args) throws IOException {
		if(args.length !=2){
			System.err.println("Please provide an <input path> and <output path>");
			System.exit(-1);
		}
		
		// 1. Create a job name "Cities and countries statistics"
		Job job = new Job();
		job.setJarByClass(CitiesAndCountriesStatistics.class); // pass the class so Hadoop will locate the relevant JAR file by looking for the JAR file containing this class 
		job.setJobName("Cities and countries statistics");
		
		// 2. Specify input paths
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		// 3. Specify output paths
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		
	}
}
