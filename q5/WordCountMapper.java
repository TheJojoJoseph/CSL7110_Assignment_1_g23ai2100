package CSL7110_Assignment_1_g23ai2100.q5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String cleanedLine = cleanText(value.toString());
        StringTokenizer tokenizer = new StringTokenizer(cleanedLine);

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken();
            context.write(new Text(word), new IntWritable(1)); // Emit word with count 1
        }
    }

    private String cleanText(String text) {
        return text.replaceAll("[^a-zA-Z0-9\s]", "").toLowerCase();
    }
}
