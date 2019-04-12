import java.io.*;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Graph {
	public static class Mapper1 extends Mapper<Object, Text, LongWritable, LongWritable>{
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			Scanner s = new Scanner(value.toString()).useDelimiter(",");
			long key2 = s.nextLong();
			long value2 = s.nextLong();	
			context.write(new LongWritable(key2), new LongWritable(value2));
			s.close();
		}
	}

	public static class Reducer1 extends Reducer<LongWritable, LongWritable, LongWritable,  LongWritable>{
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long count = 0;	
			for(LongWritable value2 : values) {
				count++;	
			}
			context.write(key, new LongWritable(count));
		}
	}
	
	public static class InterMapper extends Mapper<LongWritable, LongWritable, LongWritable, LongWritable>{
		@Override
		protected void map(LongWritable key, LongWritable value, Context context) throws IOException, InterruptedException{
			Scanner s = new Scanner(value.toString()).useDelimiter(" ");
//			long key2 = s.nextLong();
			long value2 = s.nextLong();	
//			System.out.println(value2);
			context.write(new LongWritable(value2), new LongWritable(1));
			s.close();
		}
		
	}
	
	public static class ReducerResult extends Reducer<LongWritable, LongWritable, LongWritable,  LongWritable>{
		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			System.out.println(key);
			for(LongWritable value2 : values) {
				sum = sum + value2.get();
				
			}
			context.write(key, new LongWritable(sum));
		}
	}

    public static void main ( String[] args ) throws Exception {
    	Configuration conf = new Configuration();
    	Path toDelete = new Path("temp" + args[1]);
    	
    	FileSystem hdfs = FileSystem.get(conf);
    	
    	if(hdfs.exists(toDelete)) {
    		hdfs.delete(toDelete, true);
    	}
    	
    	
    	Job job = Job.getInstance();
        job.setJobName("Job1");
        job.setJarByClass(Graph.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(LongWritable.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(Reducer1.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path("temp" + args[1]));
        job.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("Job2");
        job2.setJarByClass(Graph.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(LongWritable.class);
        job2.setMapOutputKeyClass(LongWritable.class);
        job2.setMapOutputValueClass(LongWritable.class);
        job2.setMapperClass(InterMapper.class);
        job2.setReducerClass(ReducerResult.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path("temp" + args[1]));
        FileOutputFormat.setOutputPath(job2,new Path(args[1]));
        job2.waitForCompletion(true);
		
        
   }
}
