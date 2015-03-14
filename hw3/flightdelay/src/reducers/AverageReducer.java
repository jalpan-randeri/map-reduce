package reducers;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jalpanranderi on 2/25/15.
 */
public class AverageReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        double count = 0;
        double sum = 0;

        for (Text val : values) {
            String[] tokens = val.toString().split(":");

            count = count + Double.parseDouble(tokens[0]);
            sum = sum + Double.parseDouble(tokens[1]);

        }

        context.write(new Text(""), new Text(String.valueOf(sum / count)));
        context.write(new Text(""), new Text(String.valueOf(count)));
    }
}
