package partitionar;

import model.Key;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Partitioner;


/**
 * Created by jalpanranderi on 3/14/15.
 */
public class FlightPartitioner extends Partitioner<Key, DoubleWritable> {
    @Override
    public int getPartition(Key key, DoubleWritable text, int i) {
        // equally spreads the data
        return Math.abs(key.getName().hashCode() * 127) % i;
    }
}
