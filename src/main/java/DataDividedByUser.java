import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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
    public static void main(String[] args) {

    }
}
