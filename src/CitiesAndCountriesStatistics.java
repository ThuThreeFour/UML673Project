
import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.MultipleInputs;
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
	
	public static class MapProjectionClass extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text>{
		
	    private Text district = new Text(); // type of output key
	    private Text city = new Text();  // type of output value
	    
	    @Override
	    public void map(LongWritable key, Text value, 
	    				OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
	
	    	String[] tuple = value.toString().split(",");
	    	
	    	String cityName = tuple[1];
	    	String districtName = tuple[3];

	    	district.set(districtName);
	    	city.set(cityName);
	    	System.err.println("district: " + cityName + "\tcity: " + cityName);
	    	output.collect(district, new Text(cityName)); // emit key-value pair
	    }
	}
	
	public static class MapNaturalJoinClass1 extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text>{
		
	    private Text countryCode = new Text(); // type of output key
	    private Text language = new Text(); // type of output value
	    
	    @Override
	    public void map(LongWritable key, Text value, 
	    				OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
	
	    	String[] tuple = value.toString().split(",");
	    	
	    	String countrycode = tuple[0];
	    	String languageSpoken = tuple[1];
	    	String isOfficial = tuple[2];
	    	
	    	if(languageSpoken.equals("English") && isOfficial.equals("T") ){ 
	    		countryCode.set(countrycode);
	    		language.set(languageSpoken);
	    		System.err.println("country code: " + countrycode + "\t language: " + languageSpoken);
	    		output.collect(countryCode, language); // emit key-value pair
	    	}
	    }
	}
	
	public static class MapNaturalJoinClass2 extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, Text>{
		
	    private Text CountryCode = new Text(); // type of output key
	    private Text CountryName = new Text(); // type of output value
	    
	    @Override
	    public void map(LongWritable key, Text value, 
	    				OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
	
	    	String[] tuple = value.toString().split(",");
	    	
	    	String countrycode = tuple[0];
	    	String countryname = tuple[1];
	    	

	    	CountryCode.set(countrycode);
	    	CountryName.set(countryname);
	    	System.err.println("country code: " + countrycode + "\t country name: " + countryname);
	    	output.collect(CountryCode, CountryName); // emit key-value pair

	    }
	}
	
	

	
	public static class MapAggregationClass extends MapReduceBase 
	implements Mapper<LongWritable, Text, Text, IntWritable>{
		
	    private Text district = new Text(); // type of output key
	    private final static IntWritable one = new IntWritable(1); // type of output value
	    
	    @Override
	    public void map(LongWritable key, Text value, 
	    				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
	
	    	String[] tuple = value.toString().split(",");
	    	
	    	String cityName = tuple[1];
	    	String districtName = tuple[3];

	    	district.set(districtName);
	    	//System.err.println("district: " + cityName + "\tcity: " + cityName + "\tcount: " + one);
	    	output.collect(district, one); // emit key-value pair
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
	
	public static class ReduceProjectionClass extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

		@Override
		public void reduce(Text key, Iterator<Text> values, 
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
			while(values.hasNext()){
				output.collect(key, values.next()); // create a pair <city, population>
			}
		}
	}

	public static class ReduceNaturalJoinClass extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
		
		@Override
		public void reduce(Text key, Iterator<Text> values, 
				OutputCollector<Text, Text> output, Reporter reporter) throws IOException{
			String[] currentVals = new String[2];
			int index = 0;
			System.out.println(key.toString());
			while(values.hasNext()){
				currentVals[index] = values.next().toString();
				System.out.println("index:" + index + "\tvalue: " + currentVals[index]);
				index++;
			}
			
			if(index == 2){
				if(currentVals[0].equals("English")){
					output.collect(key, new Text(currentVals[1] + "\t" + currentVals[0]));
				} else {
					output.collect(key, new Text(currentVals[0] + "\t" + currentVals[1]));
				}	
			}
			
		}
	}
	
	public static class ReduceAggregationClass extends MapReduceBase 
	implements Reducer<Text, IntWritable, Text, IntWritable>{
		
		private IntWritable citiesCounter = new IntWritable();
		
		@Override
		public void reduce(Text key, Iterator<IntWritable> values, 
				OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException{
			int sum = 0;
			
			while(values.hasNext()){
				// sum + 1
				sum += values.next().get();
			}
			
			citiesCounter.set(sum);
			
			output.collect(key,citiesCounter); // create a pair <distrcit, total number of cities>		
		}
	}
	

	  @Override
	  public int run(String[] args) throws IOException
	  {
	    return 0;
	  }	
	
	// Entry point
	  public static void main(String[] args) throws Exception {

		  // 3.1.1 Create a job name "Computing Selection by MapReduce"
		  JobConf job = new JobConf(CitiesAndCountriesStatistics.class); // pass the class so Hadoop will locate the relevant JAR file by looking for the JAR file containing this class 
		  job.setJobName("Computing Selection by MapReduce");

		  // 3.2.1 Create a job name "Computing Projection by MapReduce"
		  JobConf projectionJob = new JobConf(CitiesAndCountriesStatistics.class); // pass the class so Hadoop will locate the relevant JAR file by looking for the JAR file containing this class 
		  projectionJob.setJobName("Computing Projection by MapReduce");
		  
		  // 3.2.1 Create a job name "Computing Projection by MapReduce"
		  JobConf naturalJoinJob = new JobConf(CitiesAndCountriesStatistics.class); // pass the class so Hadoop will locate the relevant JAR file by looking for the JAR file containing this class 
		  naturalJoinJob.setJobName("Computing Natural Join by MapReduce");
		  
		  // 3.4 Create a job name Aggregation by MapReduce
		  JobConf aggregationJob = new JobConf(CitiesAndCountriesStatistics.class); // pass the class so Hadoop will locate the relevant JAR file by looking for the JAR file containing this class 
		  aggregationJob.setJobName("Aggregation by MapReduce");
		  
		  // 3.1.2
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(Text.class);
		  aggregationJob.setOutputKeyClass(Text.class);
		  aggregationJob.setOutputValueClass(IntWritable.class);
		  naturalJoinJob.setOutputKeyClass(Text.class);
		  naturalJoinJob.setOutputValueClass(Text.class);
		  projectionJob.setOutputKeyClass(Text.class);
		  projectionJob.setOutputValueClass(Text.class);
		  
		  // 3.1.3
		  job.setMapperClass(MapSelectionClass.class);
		  job.setReducerClass(ReduceSelectionClass.class);
		  projectionJob.setMapperClass(MapProjectionClass.class);
		  projectionJob.setReducerClass(ReduceProjectionClass.class);
		  naturalJoinJob.setNumMapTasks(2);
		  naturalJoinJob.setMapperClass(MapNaturalJoinClass1.class);
		  naturalJoinJob.setMapperClass(MapNaturalJoinClass2.class);
		  //projectionJob.setNumReduceTasks(1);
		  naturalJoinJob.setReducerClass(ReduceNaturalJoinClass.class);
		  projectionJob.setMapperClass(MapProjectionClass.class);
		  projectionJob.setReducerClass(ReduceProjectionClass.class);
		  aggregationJob.setMapperClass(MapAggregationClass.class);
		  aggregationJob.setReducerClass(ReduceAggregationClass.class);	
		  
		  
		  // 3.1.4
		  job.setInputFormat(TextInputFormat.class);
		  job.setOutputFormat(TextOutputFormat.class);
		  projectionJob.setInputFormat(TextInputFormat.class);
		  projectionJob.setOutputFormat(TextOutputFormat.class);
		  naturalJoinJob.setOutputFormat(TextOutputFormat.class);
		  aggregationJob.setInputFormat(TextInputFormat.class);
		  aggregationJob.setOutputFormat(TextOutputFormat.class);

		  // 3.1.5 Specify input paths
		  FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000/user/hadoop/UML673ProjectInput/city.txt"));	
		  FileInputFormat.setInputPaths(projectionJob, new Path("hdfs://localhost:9000/user/hadoop/UML673ProjectInput/city.txt"));	
		  MultipleInputs.addInputPath(naturalJoinJob, new Path("hdfs://localhost:9000/user/hadoop/UML673ProjectInput/countrylanguage.txt"),TextInputFormat.class, MapNaturalJoinClass1.class); 
		  MultipleInputs.addInputPath(naturalJoinJob, new Path("hdfs://localhost:9000/user/hadoop/UML673ProjectInput/country.txt"),TextInputFormat.class, MapNaturalJoinClass2.class); 
		  // 3.4.5
		  FileInputFormat.setInputPaths(aggregationJob, new Path("hdfs://localhost:9000/user/hadoop/UML673ProjectInput/city.txt"));	
		  
		  // 3.1.6 Specify output paths
		  FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hadoop/UML673ProjectOutput1"));
		  FileOutputFormat.setOutputPath(projectionJob, new Path("hdfs://localhost:9000/user/hadoop/UML673ProjectOutput2"));
		  FileOutputFormat.setOutputPath(naturalJoinJob, new Path("hdfs://localhost:9000/user/hadoop/UML673ProjectOutput3"));
		  FileOutputFormat.setOutputPath(aggregationJob, new Path("hdfs://localhost:9000/user/hadoop/UML673ProjectOutput4"));

		  JobClient.runJob(job);
		  JobClient.runJob(projectionJob);
		  JobClient.runJob(naturalJoinJob);
		  JobClient.runJob(aggregationJob);
	  }
}
