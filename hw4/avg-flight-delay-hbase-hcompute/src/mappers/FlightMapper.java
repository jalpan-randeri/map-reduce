package mappers;

import conts.TableConts;
import model.Key;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;


import java.io.IOException;


/**
 * Created by jalpanranderi on 3/17/15.
 */
public class FlightMapper extends TableMapper<Key, DoubleWritable> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {

        String v = new String(value.getRow());

        // Parse the row value from the HBase
        String[] tokens = v.split("\\$");

        String name = tokens[1];
        String month = new String(value.getValue(TableConts.COLUMN_FAMILY.getBytes(), TableConts.COLUMN_MONTH.getBytes()));

        String delay = new String(value.getValue(TableConts.COLUMN_FAMILY.getBytes(), TableConts.COLUMN_DELAY.getBytes()));
        double d = Double.parseDouble(delay);

        Key emit_key = new Key(name, Integer.parseInt(month));
        DoubleWritable emit_delay = new DoubleWritable(d);

        context.write(emit_key, emit_delay);
    }
}
