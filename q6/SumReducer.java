package CSL7110_Assignment_1_g23ai2100.q6;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// Reducer Class
public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable totalSum = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = calculateSum(values); // Calculate the sum of values
        totalSum.set(sum); // Set the total sum
        context.write(key, totalSum); // Emit the key and the total sum
    }

    private int calculateSum(Iterable<IntWritable> values) {
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        return sum;
    }
}
