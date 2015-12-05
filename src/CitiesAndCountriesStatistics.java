
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

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

public class CitiesAndCountriesStatistics extends Configured implements Tool{
	
	public static class MapSelectionClass extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text>{
		
	    private Text city = new Text(); // type of output key
	    private Text population = new Text(); // type of output value
	    
	    @Override
	    public void map(LongWritable key, Text value, 
	    				OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
	
	    	String[] tuple = value.toString().split(",");
	    	
	    	String cityName = tuple[1];
	    	String populationSize = tuple[4];
	    	
	    	Integer PopulationSizeInt = Integer.parseInt(populationSize);
	    	
	    	if(PopulationSizeInt > 30000){ 
	    		city.set(cityName);
	    		population.set(PopulationSizeInt.toString());
	    		System.err.println("city: " + cityName + "\t population: " + populationSize.toString());
	    		output.collect(city, population); // emit key-value pair
	    	}
	    }
	}
	
	public static class ReduceSelectionClass extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		
		@Override
		public void reduce(Text key, Iterator<Text> values, 
						   OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
	    	while(values.hasNext()){
	    		output.collect(key, values.next()); // create a pair <city, population>
	    	}
		}
	    }

	  @Override
	  public int run(String[] args) throws IOException
	  {
	    return 0;
	  }	
	
	// Entry point
	public static void main(String[] args) throws Exception {
		
		// 1. Create a job name "Cities and countries statistics"
		 JobConf job = new JobConf(CitiesAndCountriesStatistics.class); // pass the class so Hadoop will locate the relevant JAR file by looking for the JAR file containing this class 
		 job.setJobName("Cities and countries statistics");
		
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);

		 job.setMapperClass(MapSelectionClass.class);
		 job.setReducerClass(ReduceSelectionClass.class);

		 job.setInputFormat(TextInputFormat.class);
		 job.setOutputFormat(TextOutputFormat.class);
		 
		// 2. Specify input paths
		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/user/hadoop/UML673ProjectInput/city.txt"));
	
		// 3. Specify output paths
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hadoop/UML673ProjectOutput"));
	
		
		JobClient.runJob(job);		
	}
}
