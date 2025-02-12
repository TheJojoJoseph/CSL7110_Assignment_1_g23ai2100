package CSL7110_Assignment_1_g23ai2100.q4;

package com.hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import org.apache.hadoop.util.ToolRunner;

public class WordCount extends Configured implements Tool {

    private static final String JOB_NAME = "Word Count Analysis";

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            return -1;
        }

        return createAndRunJob(args[0], args[1]);
    }

    private int createAndRunJob(String inputPath, String outputPath) throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(getConf(), JOB_NAME);
        configureJob(job);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void configureJob(Job job) {
        job.setJarByClass(WordCount.class);

        // Set Mapper and Reducer classes
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        // Define output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
    }

    public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        private static final IntWritable ONE = new IntWritable(1);
        private final Text word = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String cleanedLine = cleanText(value.toString());
            processLine(cleanedLine, context);
        }

        private String cleanText(String text) {
            return text.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase();
        }

        private void processLine(String line, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(line);
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                context.write(word, ONE);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = calculateSum(values);
            writeResult(key, sum, context);
        }

        private int calculateSum(Iterable<IntWritable> values) {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            return sum;
        }

        private void writeResult(Text key, int sum, Context context) throws IOException, InterruptedException {
            result.set(sum);
            context.write(key, result);
        }
    }
}
