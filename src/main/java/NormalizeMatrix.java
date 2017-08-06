import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;

/**
 * Created by zzc on 8/4/17.
 */

public class NormalizeMatrix {
    final static Logger logger = Logger.getLogger(DataDividedByUser.class);

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String movies_id = value.toString().trim().split("-")[0];
            context.write(new Text(movies_id), value);
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            System.out.println("key:"+key);
//            Iterator<Text> iterator = values.iterator();
//            int count = 0;
//            while (iterator.hasNext()) {
//                count += iterator.next().get();
//            }
//            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String inputDataDir = args[0];
        String outputDataDir = args[1];

        File output3 = new File(outputDataDir);
        if (output3.exists()) {
            if (logger.isInfoEnabled()) {
                logger.info("Output3 directory already exits!\tDelete previous directory.");
            }
            FileUtils.deleteDirectory(output3);
        }

        Configuration conf= new Configuration();
        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
//        job.setReducerClass(NormalizeReducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        TextInputFormat.setInputPaths(job, new Path(inputDataDir));
        TextOutputFormat.setOutputPath(job, new Path(outputDataDir));
        job.waitForCompletion(true);
    }
}
