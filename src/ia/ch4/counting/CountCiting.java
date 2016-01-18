package ia.ch4.counting;

import java.io.IOException;  
import java.util.ArrayList;  
import java.util.List;  
  
import org.apache.hadoop.conf.Configuration;  
import org.apache.hadoop.conf.Configured;  
import org.apache.hadoop.fs.Path;  
import org.apache.hadoop.io.LongWritable;  
import org.apache.hadoop.io.Text;  
import org.apache.hadoop.mapreduce.Job;  
import org.apache.hadoop.mapreduce.Mapper;  
import org.apache.hadoop.mapreduce.Reducer;  
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;  
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;  
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;  
import org.apache.hadoop.util.Tool;  
import org.apache.hadoop.util.ToolRunner;  
import org.apache.hadoop.util.StringUtils; 

public class CountCiting extends Configured implements Tool{
	public static class MapClass extends Mapper<Text, Text, Text, Text> {  
        @Override  
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {  
            // "CITING","CITED"    ->  "CITED","CITING"
            context.write(value, key);  
        }  
    }  
      
    public static class Reduce extends Reducer<Text, Text, Text, Text>   
    {  
        @Override  
        public void reduce(Text key, Iterable<Text> values, Context context)  
                throws IOException, InterruptedException   
        {  
            // "CITED", "CITING List"  
            List<String> citingList = new ArrayList<String>();  
            for(Text citing:values) citingList.add(citing.toString());  
            context.write(key, new Text(StringUtils.join(",", citingList)));  
        }  
    }
	
	@Override
	public int run(String[] args) throws Exception {  
		Job job = new Job(getConf());  
        Path in = new Path(args[0]);  
        Path out = new Path(args[1]);  
        FileInputFormat.setInputPaths(job, in);  
        FileOutputFormat.setOutputPath(job, out);  
        job.setJobName("CountCiting");  
        job.setJarByClass(CountCiting.class);  
        job.setMapperClass(MapClass.class);  
        job.setReducerClass(Reduce.class);  
        job.setInputFormatClass(KeyValueTextInputFormat.class);  
        job.setOutputFormatClass(TextOutputFormat.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(Text.class);  
        job.getConfiguration().set("key.value.separator.in.input.line", ",");  
        boolean success = job.waitForCompletion(true);    
        return(success ? 0 : 1);
	}

	public static void main(String[] args) throws Exception {  
        int res = ToolRunner.run(new Configuration(), new CountCiting(), args);  
        System.exit(res);  
    } 
}
