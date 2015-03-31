package reducer;

import model.Key;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by jalpanranderi on 3/13/15.
 */
public class FlightReducer extends Reducer<Key, DoubleWritable, Text, Text> {

    @Override
    protected void reduce(Key key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

        double[] avg = calculateDelay(key, values);
        String ans = formatOutput(key, avg);

        context.write(new Text(ans), new Text());


    }

    /**
     * formatOutput formats output in the specified way
     *
     * @param key NameMonthPair
     * @param avg Double[]
     * @return String
     */
    private String formatOutput(Key key, double[] avg) {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("%s, ", key.getName()));
        int i = 0;
        for (; i < avg.length - 1; i++) {
            builder.append(String.format("(%d,%2.0f), ", i + 1, Math.ceil(avg[i])));
        }
        builder.append(String.format("(%d,%2.0f)", i + 1, Math.ceil(avg[i])));
        return builder.toString();
    }

    /**
     * calculate Delay for the given list of values
     *
     * @param key    NameMonthPair
     * @param values List<Double>
     * @return avg Double[]
     */
    private double[] calculateDelay(Key key, Iterable<DoubleWritable> values) {
        double[] avg = new double[12];
        int prev_month = key.getMonth();
        double sum = 0;
        int count = 0;

        for (DoubleWritable t : values) {
            if (prev_month != key.getMonth()) {
                avg[prev_month - 1] = sum / count;
                sum = t.get();
                prev_month = key.getMonth();
                count = 1;
            } else {
                sum = sum + t.get();
                count++;
            }
        }
        avg[prev_month - 1] = sum / count;

        return avg;
    }
}
