package mappers;

import com.opencsv.CSVParser;
import conts.FlightConts;
import conts.TableConts;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by jalpanranderi on 3/17/15.
 */
public class FlightMapper extends Mapper<Object, Text, ImmutableBytesWritable, Writable> {

    private static final int MB_100 = 102400;

    private CSVParser mParser;
    private HTable mTable;
    private StringBuilder mKeyBuilder;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mParser = new CSVParser();
        mTable = new HTable(HBaseConfiguration.create(), TableConts.TABLE_NAME);
        mTable.setAutoFlush(true);
        mTable.setWriteBufferSize(MB_100);
        mKeyBuilder = new StringBuilder();
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] tokens = mParser.parseLine(value.toString());
        if(isValid(tokens)) {
            String row_key = getKey(tokens);

            byte[] delay = Bytes.toBytes(tokens[FlightConts.INDEX_DELAY]);
            byte[] diverted = Bytes.toBytes(tokens[FlightConts.INDEX_DIVERTED]);
            byte[] cancelled = Bytes.toBytes(tokens[FlightConts.INDEX_CANCELED]);
            byte[] month = Bytes.toBytes(tokens[FlightConts.INDEX_MONTH]);
            byte[] year = Bytes.toBytes(tokens[FlightConts.INDEX_YEAR]);

            Put row = new Put(Bytes.toBytes(row_key));
            row.add(TableConts.COLUMN_FAMILY.getBytes(), TableConts.COLUMN_CANCELLED.getBytes(), cancelled);
            row.add(TableConts.COLUMN_FAMILY.getBytes(), TableConts.COLUMN_DIVERTED.getBytes(), diverted);
            row.add(TableConts.COLUMN_FAMILY.getBytes(), TableConts.COLUMN_DELAY.getBytes(), delay);
            row.add(TableConts.COLUMN_FAMILY.getBytes(), TableConts.COLUMN_MONTH.getBytes(), month);
            row.add(TableConts.COLUMN_FAMILY.getBytes(), TableConts.COLUMN_YEAR.getBytes(), year);

            mTable.put(row);
        }

    }

    private static boolean isValid(String[] tokens){
        return !(tokens[FlightConts.INDEX_DELAY].isEmpty() ||
                tokens[FlightConts.INDEX_AIRLINE].isEmpty() ||
                tokens[FlightConts.INDEX_CANCELED].isEmpty() ||
                tokens[FlightConts.INDEX_DATE].isEmpty() ||
                tokens[FlightConts.INDEX_FLIGHT_NUM].isEmpty() ||
                tokens[FlightConts.INDEX_SRC].isEmpty() ||
                tokens[FlightConts.INDEX_MONTH].isEmpty() ||
                tokens[FlightConts.INDEX_DIVERTED].isEmpty());
    }

    private String getKey(String[] tokens) {
        mKeyBuilder.setLength(0);
        mKeyBuilder.append(tokens[FlightConts.INDEX_AIRLINE]);
        mKeyBuilder.append(TableConts.SEPARATOR);
        mKeyBuilder.append(tokens[FlightConts.INDEX_DATE]);
        mKeyBuilder.append(TableConts.SEPARATOR);
        mKeyBuilder.append(tokens[FlightConts.INDEX_FLIGHT_NUM]);
        mKeyBuilder.append(TableConts.SEPARATOR);
        mKeyBuilder.append(tokens[FlightConts.INDEX_SRC]);
        ;

        return mKeyBuilder.toString();
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        mTable.close();

    }
}
