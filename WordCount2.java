import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//hw3 1515009 kim da young
public class WCount {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//first mapper class
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}

	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//first reducer class
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,  Context context)
				throws IOException, InterruptedException {

			int sum = 0;
			for (IntWritable val : values) {

				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}

	}
	
	public static class FinMapper extends Mapper< Text, Text, IntWritable, Text> {
//second mapper class
		  IntWritable frequency = new IntWritable();
		  @Override
		  public void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		    int newVal = Integer.parseInt(value.toString());
		    frequency.set(newVal);
		    context.write(frequency, key);
		  }
		}
//output : (intwritable, text)
	public static class FinReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
//second reducer class
		  Text word = new Text();

		  @Override
		  public void reduce(IntWritable key, Iterable<Text> values, Context context)
		        throws IOException, InterruptedException {

		    for (Text value : values) {
		        word.set(value);
		        context.write(key, word);
		    }
		  }
		}
	//key is intwritable type and value is the text type
	public class WordCount extends Configured implements Tool {
//first job
		public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		Job job1 = new Job(conf, "WordCount");

		
		
		job1.setJarByClass(WCount.class);
		job1.setMapperClass(MyMapper.class);
		job1.setReducerClass(MyReducer.class);

		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);

		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		
		return job1.waitForCompletion(true) ? 0 : 1;
		  }
	}
	
	public class WordCount2 extends Configured implements Tool {
			public int run(String[] args) throws Exception {
	//second job	
		Configuration conf = getConf();
		
		
		Job job2 = new Job(conf, "WordCount2");

		job2.setJarByClass(WCount.class);
		job2.setMapperClass(FinMapper.class);
		job2.setReducerClass(FinReducer.class);
		
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);

		job2.setInputFormatClass(KeyValueTextInputFormat.class);
		job2.setSortComparatorClass(IntComparator.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path(args[1]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		
		return job2.waitForCompletion(true) ? 0 : 1;
		  }
	}
	
	public static class IntComparator extends WritableComparator {
//to use intwritable type as the key
		  public IntComparator() {
		    super(IntWritable.class);
		  }

		  @Override
		  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		    Integer v1 = ByteBuffer.wrap(b1, s1, l1).getInt();
		    Integer v2 = ByteBuffer.wrap(b2, s2, l2).getInt();
		    return v1.compareTo(v2) * (-1);
		  }
		}
	
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
	//job chaining	  
		  ToolRunner.run(new WCount().new WordCount(), args);
		  ToolRunner.run(new WCount().new WordCount2(), args);
		 }
			
			

	

}
