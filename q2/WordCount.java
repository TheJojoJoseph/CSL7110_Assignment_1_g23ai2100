package CSL7110_Assignment_1_g23ai2100.q2;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    // Mapper class to emit byte offset as key and line of text as value
    public static class OffsetLineMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) 
                throws IOException, InterruptedException {
            // Emit the byte offset as key and the line of text as value
            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        // Validate input arguments
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(1);
        }

        // Create a Hadoop configuration and job instance
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Line with Offsets");

        // Set the Jar by class to ensure the correct jar is used
        job.setJarByClass(WordCount.class);

        // Set the Mapper class
        job.setMapperClass(OffsetLineMapper.class);

        // Disable the Reducer since this is a map-only job
        job.setNumReduceTasks(0);

        // Set the output key and value classes
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        // Set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for completion
        boolean success = job.waitForCompletion(true);
        System.exit(success ? 0 : 1);
    }
}
