package mapper;

import com.opencsv.CSVParser;
import conts.FlightConst;
import model.Key;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jalpanranderi on 3/13/15.
 */
public class FlightMapper extends Mapper<Object, Text, Key, DoubleWritable> {

    private static final String DIVERTED = "1";
    private static final String CANCELLED = "1";
    private static final String YEAR = "2008";
    private CSVParser mParser = new CSVParser();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = mParser.parseLine(value.toString());

        if (isValid(data) &&
                !(data[FlightConst.INDEX_DIVERTED].equals(DIVERTED) ||
                        data[FlightConst.INDEX_CANCELED].equals(CANCELLED)) &&
                data[FlightConst.INDEX_YEAR].equals(YEAR)) {

            Key mKey = new Key(data[FlightConst.INDEX_AIRLINE], Integer.parseInt(data[FlightConst.INDEX_MONTH]));
            double mValue = Double.parseDouble(data[FlightConst.INDEX_DELAY]);

            context.write(mKey, new DoubleWritable(mValue));
        }
    }

    /**
     * isValid checks for empty values in input
     *
     * @param data String[] input
     * @return true iff input does not contains any empty values
     */
    private boolean isValid(String[] data) {
        return !(data[FlightConst.INDEX_MONTH].isEmpty() ||
                data[FlightConst.INDEX_AIRLINE].isEmpty() ||
                data[FlightConst.INDEX_CANCELED].isEmpty() ||
                data[FlightConst.INDEX_DIVERTED].isEmpty() ||
                data[FlightConst.INDEX_DELAY].isEmpty());
    }
}
