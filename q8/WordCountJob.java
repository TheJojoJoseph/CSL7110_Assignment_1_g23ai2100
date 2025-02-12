package CSL7110_Assignment_1_g23ai2100.q8;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountJob {

    private static final String JOB_NAME_PREFIX = "Word Count - Split Size: ";
    private static final String MB_SUFFIX = "MB";
    private static final long BYTES_PER_MB = 1024 * 1024;

    public static boolean executeWordCountJob(String inputPath, String outputPath, long splitSizeMB) throws Exception {
        // Configure job settings
        Configuration config = createJobConfiguration(splitSizeMB);
        Job job = configureJob(config, splitSizeMB, inputPath, outputPath);

        // Execute job and measure performance
        JobExecutionResult result = executeAndMeasureJob(job, splitSizeMB);

        // Log results
        logJobResults(result);

        return result.isSuccessful();
    }

    private static Configuration createJobConfiguration(long splitSizeMB) {
        Configuration config = new Configuration();
        config.setLong("mapreduce.input.fileinputformat.split.maxsize", splitSizeMB * BYTES_PER_MB);
        return config;
    }

    private static Job configureJob(Configuration config, long splitSizeMB,
            String inputPath, String outputPath) throws IOException {
        Job job = Job.getInstance(config, JOB_NAME_PREFIX + splitSizeMB + MB_SUFFIX);

        // Set job classes
        job.setJarByClass(WordCountJob.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // Set output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // Configure input/output paths
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath + "_" + splitSizeMB));

        return job;
    }

    private static JobExecutionResult executeAndMeasureJob(Job job, long splitSizeMB) throws Exception {
        long startTime = System.currentTimeMillis();
        boolean success = job.waitForCompletion(true);
        long endTime = System.currentTimeMillis();

        return new JobExecutionResult(success, splitSizeMB, endTime - startTime);
    }

    private static void logJobResults(JobExecutionResult result) {
        if (result.isSuccessful()) {
            System.out.println("Job with Split Size " + result.getSplitSizeMB() + MB_SUFFIX
                    + " completed successfully!");
            System.out.println("Total execution time: " + result.getExecutionTimeMs() + " ms\n");
        } else {
            System.out.println("Job with Split Size " + result.getSplitSizeMB() + MB_SUFFIX + " failed.");
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected 2 arguments: inputPath and outputPath");
        }

        String inputPath = args[0];
        String outputPath = args[1];
        long[] splitSizes = {1, 2, 4, 8}; // Split sizes in MB

        for (long splitSize : splitSizes) {
            executeWordCountJob(inputPath, outputPath, splitSize);
        }
    }
}

/**
 * Mapper class for tokenizing words
 */
class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
        }
    }
}

/**
 * Reducer class for summing word counts
 */
class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        result.set(sum);
        context.write(key, result);
    }
}

/**
 * Immutable class to hold job execution results
 */
class JobExecutionResult {

    private final boolean successful;
    private final long splitSizeMB;
    private final long executionTimeMs;

    public JobExecutionResult(boolean successful, long splitSizeMB, long executionTimeMs) {
        this.successful = successful;
        this.splitSizeMB = splitSizeMB;
        this.executionTimeMs = executionTimeMs;
    }

    public boolean isSuccessful() {
        return successful;
    }

    public long getSplitSizeMB() {
        return splitSizeMB;
    }

    public long getExecutionTimeMs() {
        return executionTimeMs;
    }
}
