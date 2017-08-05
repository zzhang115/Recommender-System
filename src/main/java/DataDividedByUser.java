import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

/**
 * Created by zzc on 8/4/17.
 */
public class DataDividedByUser {
    public static class DataDivideMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

//    public static class DataDivideReducer extends Reducer<> {
//
//    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputDataDir = args[0];
        String outputDataDir = args[1];

        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(DataDivideMapper.class);
//        job.setReducerClass(DataDivideReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(inputDataDir));
        TextOutputFormat.setOutputPath(job, new Path(outputDataDir));
        job.waitForCompletion(true);
    }
}
