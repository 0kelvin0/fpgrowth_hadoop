package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.TaskCounter;

import aggregator.AggregatorMapper;
import aggregator.AggregatorReducer;
import aggregator2.Aggregator2Mapper;
import aggregator2.Aggregator2Reducer;
import parallelCounting.ParallelCountingMapper;
import parallelCounting.ParallelCountingReducer;
import parallelFpGrowth.ParallelFpMapper;
import parallelFpGrowth.ParallelFpReducer;
import utils.Pair;
import utils.SortByCount;
import utils.SplitByNumberOfMappersTextInputFormat;

public class FpGrowth extends Configured implements Tool {
	
	public static final String NUMBER_OF_LINES_KEY = "number_of_lines_read";
	public static final String SUBDB_SIZE = "sub_db_size";
	private final static int noOfGroups = 10000;
	private String[] moreParas;
	private static String dataBaseName;
	private static double relativeMinSupport;
	private String input_file;
	private String output_dir;
	private static int dataSize;
	private static int numMappers;
	private static int childJavaOpts;
	private static int  minsup;
	private static int maxPatterns;
	
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("outputFile", output_dir);
		conf.set("noOfGroups", String.valueOf(noOfGroups));
		conf.set("minSupport", String.valueOf(minsup));
		conf.setInt("numMappers", numMappers);
		conf.set("maxPatterns", String.valueOf(maxPatterns));
		conf.set("mapreduce.task.timeout", "6000000");
		conf.set("mapreduce.map.java.opts", "-Xmx"+childJavaOpts+"M");
		conf.set("mapreduce.reduce.java.opts", "-Xmx"+(childJavaOpts*2)+"M");

		try {
			cleanDirs(new String[] {output_dir});
			long start = System.currentTimeMillis();
			long nrLines = startParallelCounting(conf,input_file,output_dir);
			System.out.println("nrLines: " + nrLines);
			//startParallelCounting(conf,input_file,output_dir);
			List<Pair> fList = getFList(output_dir);
			saveFlist(fList,conf,output_dir);
			startParallelFpGrowth(conf,input_file,output_dir, nrLines);
			startAggregation(conf,output_dir);
			//startFinalAggregation(conf,output_dir);  //just alternative output format
			long end = System.currentTimeMillis();
			System.out.println("\n Total time taken is " + ((end - start) / 1000.0) +"\n");
			saveResult((end - start) / 1000.0);
		} catch(Exception e) {
		    System.out.println(e);
		}
		return 0;
	}
	
	/* save F-list to hdfs in a file */
	protected static void saveFlist(List<Pair> fList,Configuration conf,String outputFile) throws IOException {
		String destination = outputFile + "/" + "pc" + "/"+ "fList";
		FileSystem fs = FileSystem.get(URI.create(destination), conf);
		OutputStream out = fs.create(new Path(destination));
		BufferedWriter br = new BufferedWriter( new OutputStreamWriter(out) );
		for(Pair itemAndCount: fList) {
			br.write(itemAndCount.getItem() + " " + itemAndCount.getCount() + "\n");
		}
		br.close();
		fs.close();
	}
	
	/* read F-list from hdfs */
	public static List<Pair> readFList(Configuration conf) throws IOException{
		Path p = new Path(conf.get("outputFile")+ "/" + "pc" + "/"+"fList");
		FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
        String line;
        line = br.readLine();
        List<Pair> fList = new ArrayList<Pair>();
        while (line != null){
        	StringTokenizer s1 = new StringTokenizer(line);
        	String item = s1.nextToken();
        	long count  = Long.parseLong(s1.nextToken());
        	fList.add(new Pair(item,count));
        	line = br.readLine();
        }
        return fList;
		/*Path[] files = DistributedCache.getLocalCacheFiles(conf);
        FileSystem fs = FileSystem.getLocal(conf);
        Path fListLocalPath = fs.makeQualified(files[0]);
        URI[] filesURIs = DistributedCache.getCacheFiles(conf);
        fListLocalPath = new Path(filesURIs[0].getPath());*/
        /*for (Pair itemAndCount : new SequenceFileIterable<Text, LongWritable>(fListLocalPath, true, conf)) {
            fList.add(new Pair(itemAndCount.getItem().toString(),itemAndCount.getCount()));
        }
        return fList;*/
	}
	
	protected static void startParallelFpGrowth(Configuration conf,String inputFile,String outputFile,long nrLines) throws IOException,InterruptedException,ClassNotFoundException{
		//Job job = new Job(conf,"parallelFpGrowth");
		int subDbSize = (int) Math.ceil(1.0 * nrLines / numMappers);
		conf.setLong(NUMBER_OF_LINES_KEY, nrLines);
		conf.setInt(SUBDB_SIZE, subDbSize);
		Job job = Job.getInstance(conf, "parallelFpGrowth");
		job.setJarByClass(FpGrowth.class);
		job.setMapperClass(ParallelFpMapper.class);
	   	job.setReducerClass(ParallelFpReducer.class);
	   	job.setOutputKeyClass(Text.class);
	   	job.setOutputValueClass(LongWritable.class);
	   	job.setMapOutputKeyClass(IntWritable.class);
	   	job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(SplitByNumberOfMappersTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);	
		job.setNumReduceTasks(1); 
	   	FileInputFormat.addInputPath(job, new Path(inputFile)); 
	   	FileOutputFormat.setOutputPath(job,new Path(outputFile+ "/" + "pfp" ));
	   	if(job.waitForCompletion(true) == true) {
	   		return;
	   	}
	}
	
	/* reads items and count from HDFS after 1st map reduce, sorts it according to count(desc) and returns it*/
	protected static List<Pair> getFList(String outputFile) {
		try {
			Path p = new Path(outputFile + "/" + "pc" + "/" +"part-r-00000");
			FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(p)));
            String line;
            line = br.readLine();
            List<Pair> fList = new ArrayList<Pair>();
            while (line != null){
            	StringTokenizer s1 = new StringTokenizer(line);
            	String item = s1.nextToken();
            	long count  = Long.parseLong(s1.nextToken());
            	fList.add(new Pair(item,count));
            	line = br.readLine();
            }
            Collections.sort(fList,new SortByCount());
            return fList;
		}
		catch(Exception e) {
			System.out.println(e);
			return null;
		}
	}
	
	/* get count of all items in transactions */
	protected static long startParallelCounting(Configuration conf,String inputFile,String outputFile) throws IOException,InterruptedException,ClassNotFoundException{
		long nrLines = -1;
		String outputDir = outputFile + "/" + "pc";
		//Job job = new Job(conf, "countItems");
		Job job = Job.getInstance(conf, "countItems");
		job.setJarByClass(FpGrowth.class);
		
		job.setMapperClass(ParallelCountingMapper.class);
	   	job.setReducerClass(ParallelCountingReducer.class);
	   	job.setOutputKeyClass(Text.class);
	   	job.setOutputValueClass(LongWritable.class);
	   	
	   	FileInputFormat.addInputPath(job, new Path(inputFile)); 
	   	FileOutputFormat.setOutputPath(job,new Path(outputDir));
	   	
		job.setInputFormatClass(SplitByNumberOfMappersTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);	
		job.setNumReduceTasks(1);   
	   	if(job.waitForCompletion(true) == true) {
	   		//return;
			nrLines = job.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
			if (nrLines != -1) {
				conf.setLong(NUMBER_OF_LINES_KEY, nrLines);
			}
	   	}
		return nrLines;
	}
	
	/* aggregation of patterns so that only unique patterns are present */
	protected static void startAggregation(Configuration conf,String outputFile) throws IOException,InterruptedException,ClassNotFoundException{
		//Job job = new Job(conf,"aggregating");
		Job job = Job.getInstance(conf, "aggregating");
		job.setJarByClass(FpGrowth.class);
		
		job.setMapperClass(Aggregator2Mapper.class);
		job.setReducerClass(Aggregator2Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
	   	
	   	FileInputFormat.addInputPath(job, new Path(outputFile + "/" + "pfp" + "/" +"part-r-00000")); 
	   	FileOutputFormat.setOutputPath(job,new Path(outputFile + "/" + "ag"));
	   	
	   	if(job.waitForCompletion(true) == true) {
	   		return;
	   	}
	}
	
	/* final aggregation where all unique frequent itemsets have their frequent patterns */
	protected static void startFinalAggregation(Configuration conf,String outputFile) throws IOException,InterruptedException,ClassNotFoundException{
		//Job job = new Job(conf,"finalAggregation");
		Job job = Job.getInstance(conf, "finalAggregation");
		job.setJarByClass(FpGrowth.class);
		
		job.setMapperClass(AggregatorMapper.class);
		job.setReducerClass(AggregatorReducer.class);
		job.setOutputKeyClass(Text.class);
	   	job.setOutputValueClass(Text.class);
	   	
	   	//FileInputFormat.addInputPath(job, new Path(outputFile + "/" + "ag" + "/" + "part-r-00000")); 
		FileInputFormat.addInputPath(job, new Path(outputFile + "/" + "pfp" + "/" +"part-r-00000")); 
	   	FileOutputFormat.setOutputPath(job,new Path(outputFile + "/" + "fis"));
	   	
	   	if(job.waitForCompletion(true) == true) {
	   		return;
	   	}
	}

  	public static void cleanDirs(String... files) {
	    System.out.println("[Cleaning]: Cleaning HDFS before running the algorithm");
	    Configuration conf = new Configuration();
	    for (String filename : files) {
	      System.out.println("[Cleaning]: Trying to delete " + filename);
	      Path path = new Path(filename);
	      try {
	        FileSystem fs = path.getFileSystem(conf);
	        if (fs.exists(path)) {
	          if (fs.delete(path, true)) {
	            System.out.println("[Cleaning]: Deleted " + filename);
	          } else {
	            System.out.println("[Cleaning]: Error while deleting " + filename);
	          }
	        } else {
	          System.out.println("[Cleaning]: " + filename + " does not exist on HDFS");
	        }
	      } catch (IOException e) {
	        e.printStackTrace();
	      }
	    }
	}

	public static void saveResult(double totalTime){
		try{
			BufferedWriter br = null;
			
			File resultFile = new File("FPGrowth_" + dataBaseName + "_ResultOut");
			if(!resultFile.exists()){
				br  = new BufferedWriter(new FileWriter(resultFile, true));
				br.write("algo" + "\t\t" + "dataset" + "\t\t" + "DBSize" + "\t" + "minSup(%)" + "\t" + "#Mappers" + "\t" + "maxPatterns"  + "\t" + "TotalTime"  + "\t");
				br.write("\n");
			}else{
				br  = new BufferedWriter(new FileWriter(resultFile, true));
			}
			br.write("============================================================================================================================");
			br.write("\n");
			
			br.write("FPGrowth" + "\t" + dataBaseName + "\t" + dataSize + "\t" + relativeMinSupport * 100.0 + "\t\t" + numMappers + "\t\t" + maxPatterns + "\t\t" + totalTime + "\t");
			br.write("\n");
			br.write("============================================================================================================================");
			br.write("\n");
			br.flush();
			br.close();
		}catch(Exception e){
			System.out.println(e);
		}
	}

	public FpGrowth(String[] args) {
		int numFixedParas = 8; // datasetName, minsup(relative), input_file, output_file, 
		                       // datasize, childJavaOpts, nummappers, maxPatterns.
		int numMoreParas = args.length - numFixedParas;
		if (args.length < numFixedParas || numMoreParas % 2 != 0) {
			System.out.println("The Number of the input parameters is Wrong!!");
			System.exit(1);
		} else {
			if (numMoreParas > 0) {
				moreParas = new String[numMoreParas];
				for (int k = 0; k < numMoreParas; k++) {
					moreParas[k] = args[numFixedParas + k];
				}
			} else {
				moreParas = new String[1];
			}
		}
		dataBaseName = args[0];
		relativeMinSupport = Double.parseDouble(args[1]);
		input_file = args[2];
		output_dir = args[3];
		dataSize = Integer.parseInt(args[4]);
		minsup = (int)Math.ceil(relativeMinSupport * dataSize);
		childJavaOpts = Integer.parseInt(args[5]);
		numMappers = Integer.parseInt(args[6]);
		maxPatterns = Integer.parseInt(args[7]);

	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new FpGrowth(args), args);
		System.out.println(res);
	}
}
