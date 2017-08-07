import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Created by zzc on 8/6/17.
 */
public class Multiplication {
    final static Logger logger = Logger.getLogger(Multiplication.class);

    public static class CocurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        }
    }

    public static class MultiplicationReducer extends Reducer<LongWritable, Text, Text, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String coCurrenceInputFileDir = args[0];
        String ratingsInputFileDir = args[1];
        String outputFileDir = args[2];

        File output4 = new File(outputFileDir);
        if (output4.exists()) {
            if (logger.isInfoEnabled()) {
                logger.info("Output4 directory already exits!\tDelete previous directory.");
            }
            FileUtils.deleteDirectory(output4);
        }

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        job.setJarByClass(Multiplication.class);

        ChainMapper.addMapper(job, CocurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, configuration);
        ChainMapper.addMapper(job, RatingMapper.class, LongWritable.class, Text.class, Text.class, Text.class, configuration);

        job.setMapperClass(CocurrenceMapper.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(MultiplicationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(coCurrenceInputFileDir), TextInputFormat.class, CocurrenceMapper.class);
        MultipleInputs.addInputPath(job, new Path(ratingsInputFileDir), TextInputFormat.class, RatingMapper.class);

        TextOutputFormat.setOutputPath(job, new Path(outputFileDir));
        job.waitForCompletion(true);
    }
}
