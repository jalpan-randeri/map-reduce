package mappers;

import com.opencsv.CSVParser;
import conts.FlightConts;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by jalpanranderi on 2/23/15.
 */
public class FlightMapper extends Mapper<Object, Text, Text, Text> {

    public static final String FORMAT = "yyyy-MM-dd";
    public static final String DATE_START = "2007-6-1";
    public static final String DATE_END = "2008-5-31";

    private Date mStartDate;
    private Date mEndDate;
    private CSVParser mParser;
    private SimpleDateFormat mSdf;


    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mSdf = new SimpleDateFormat(FORMAT);
        mParser = new CSVParser();

        try {
            mStartDate = mSdf.parse(DATE_START);
            mEndDate = mSdf.parse(DATE_END);
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = mParser.parseLine(value.toString());

        if (!isFlightDelayedOrDiverted(data)
                && isFlightInSpecifiedInterval(data[FlightConts.INDEX_DATE])
                && isAllFieldPresent(data)) {

            if (data[FlightConts.INDEX_SRC].equals(FlightConts.ORIGIN) &&
                    !data[FlightConts.INDEX_DEST].equals(FlightConts.DESTINATION)) {

                Text k2 = new Text(generateKey(data[FlightConts.INDEX_DEST],
                        data[FlightConts.INDEX_DATE]));
                Text v2 = new Text(generateValue(data[FlightConts.INDEX_SRC],
                        data[FlightConts.INDEX_ARR_TIME],
                        data[FlightConts.INDEX_DELAY]));

                context.write(k2, v2);
            } else if (data[FlightConts.INDEX_DEST].equals(FlightConts.DESTINATION) &&
                    !data[FlightConts.INDEX_SRC].equals(FlightConts.ORIGIN)) {

                Text k2 = new Text(generateKey(data[FlightConts.INDEX_SRC], data[FlightConts.INDEX_DATE]));
                Text v2 = new Text(String.format("%s,%s,%s", data[FlightConts.INDEX_DEST],
                        data[FlightConts.INDEX_DEP_TIME], data[FlightConts.INDEX_DELAY]));

                context.write(k2, v2);
            }
        }
    }

    /**
     * generateValue create the value based on the parameters
     *
     * @param field1 String
     * @param field2 String
     * @param field3 String
     * @return String concatenation of all fields.
     */
    private String generateValue(String field1, String field2, String field3) {
        return String.format("%s,%s,%s", field1, field2, field3);
    }


    /**
     * generateKey create the key based on teh parameters
     *
     * @param field1 String
     * @param field2 String
     * @return String concatenation of all fields.
     */
    private String generateKey(String field1, String field2) {
        return String.format("%s,%s", field1, field2);
    }

    /**
     * isAllFieldPresent
     *
     * @param data String[] containing the line read from input file
     * @return Boolean true iff all fields are present
     */
    private boolean isAllFieldPresent(String[] data) {
        return !data[FlightConts.INDEX_DELAY].isEmpty() &&
                !data[FlightConts.INDEX_ARR_TIME].isEmpty() &&
                !data[FlightConts.INDEX_DEP_TIME].isEmpty();
    }


    /**
     * determines the given date is within range or not
     *
     * @param date String representing date
     * @return Boolean true if date is in range
     */
    private boolean isFlightInSpecifiedInterval(String date) {
        try {
            return isWithinRange(mSdf.parse(date));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * determines the given date against range
     *
     * @param testDate Date
     * @return true if date is in range
     */
    private boolean isWithinRange(Date testDate) {
        return !(testDate.before(mStartDate) || testDate.after(mEndDate));
    }

    /**
     * determines the status of flight
     *
     * @param s String[] representing data
     * @return true iff flight is delayed or diverted
     */
    public boolean isFlightDelayedOrDiverted(String[] s) {
        return s[FlightConts.INDEX_CANCELED].equals("1") ||
                s[FlightConts.INDEX_DIVERTED].equals("1");
    }
}
