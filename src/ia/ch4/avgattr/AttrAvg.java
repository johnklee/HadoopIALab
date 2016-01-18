package ia.ch4.avgattr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AttrAvg extends Configured implements Tool{
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {  
        @Override  
        public void map(LongWritable key, Text value, Context context) 
        		throws IOException, InterruptedException {  
        	// Output: country\tnumClaims\t1
        	String fields[] = value.toString().split(",", -20);
        	String country = fields[4];
        	String numClaims = fields[8];
        	if (numClaims.length() > 0 && !numClaims.startsWith("\"")) {
        		context.write(new Text(country),
        					   new Text(numClaims + ",1"));
        	} 
        }  
    }  
	
	public static class Combine extends Reducer<Text, Text, Text, Text> {
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			for (Text value : values) {
				String fields[] = value.toString().split(",");
				sum += Double.parseDouble(fields[0]);
				count += Integer.parseInt(fields[1]);
			}
			context.write(key, new Text(sum + "," + count));
		}
	}
      
    public static class Reduce extends Reducer<Text, Text, Text, DoubleWritable>   
    {  
    	@Override  
        public void reduce(Text key, Iterable<Text> values, Context context)  
                throws IOException, InterruptedException   
        {  
    		double sum = 0;
    		int count = 0;
    		for(Text value:values)
    		{
    			String fields[] = value.toString().split(",");
    			sum += Double.parseDouble(fields[0]);
    			count += Integer.parseInt(fields[1]);
    		}
    		context.write(key, new DoubleWritable(sum/count));
        }  
    }
    
    @Override
	public int run(String[] args) throws Exception {  
		Job job = new Job(getConf());  
        Path in = new Path(args[0]);  
        Path out = new Path(args[1]);  
        FileInputFormat.setInputPaths(job, in);  
        FileOutputFormat.setOutputPath(job, out);  
        job.setJobName("AttrAvg");  
        job.setJarByClass(AttrAvg.class);  
        job.setMapperClass(MapClass.class);  
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);  
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class); 
        job.setOutputFormatClass(TextOutputFormat.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(DoubleWritable.class);    
        boolean success = job.waitForCompletion(true);    
        return(success ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {  
        int res = ToolRunner.run(new Configuration(), new AttrAvg(), args);  
        System.exit(res);  
    }
}
