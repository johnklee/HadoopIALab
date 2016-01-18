package ia.ch4.counting;

import ia.ch4.counting.CountCiting.MapClass;
import ia.ch4.counting.CountCiting.Reduce;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class CitationHistogram extends Configured implements Tool{
	public static class MapClass extends Mapper<Text, Text, IntWritable, IntWritable> {  
		private final static IntWritable uno = new IntWritable(1);  
        private IntWritable citationCount = new IntWritable();  
          
        @Override  
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {  
            // PatentID, CitingNumber  
            citationCount.set(Integer.valueOf(value.toString()));  
            context.write(citationCount, uno);  
        }  
    }  
      
    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>   
    {  
    	@Override  
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)  
                throws IOException, InterruptedException   
        {  
            // CitingNumber, Count  
            int count=0;          
            Iterator<IntWritable> iter = values.iterator();  
            while(iter.hasNext())   
            {  
                iter.next();  
                count++;  
            }  
            context.write(key, new IntWritable(count));  
        } 
    }
	
	@Override
	public int run(String[] args) throws Exception {  
		Job job = new Job(getConf());  
        Path in = new Path(args[0]);  
        Path out = new Path(args[1]);  
        FileInputFormat.setInputPaths(job, in);  
        FileOutputFormat.setOutputPath(job, out);  
        job.setJobName("CitationHistogram");  
        job.setJarByClass(CitationHistogram.class);  
        job.setMapperClass(MapClass.class);  
        job.setReducerClass(Reduce.class);  
        job.setMapOutputKeyClass(IntWritable.class);  
        job.setMapOutputValueClass(IntWritable.class);  
        job.setInputFormatClass(KeyValueTextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
        job.setOutputKeyClass(IntWritable.class);  
        job.setOutputValueClass(IntWritable.class);          
        boolean success = job.waitForCompletion(true);    
        return(success ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {  
        int res = ToolRunner.run(new Configuration(), new CitationHistogram(), args);  
        System.exit(res);  
    } 
}
