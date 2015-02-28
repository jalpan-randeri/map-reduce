package mappers;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jalpanranderi on 2/25/15.
 */
public class AverageMapper extends Mapper<Object, Text, Text, Text> {

    double mSum;
    double mCount;

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        String value = mCount + ":" + mSum;
        context.write(new Text("Dummy"), new Text(value));
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        mSum = mSum + Double.parseDouble(value.toString().trim());
        mCount++;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mSum = 0;
        mCount = 0;
    }
}
