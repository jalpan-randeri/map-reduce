package reducers;

import conts.FlightConts;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by jalpanranderi on 2/24/15.
 */
public class FlightReducer extends Reducer<Text, Text, Text, Text> {


    private final int INDEX_TIME = 1;
    private final int INDEX_DELAY = 2;



    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {




         List<String[]> origins = new ArrayList<String[]>();
         List<String[]> destinations = new ArrayList<String[]>();

        for(Text t : values){
            String[] val = t.toString().split(",");

            if(val[0].equals(FlightConts.ORIGIN)){
                origins.add(val);
            }else{
                destinations.add(val);
            }
        }

        for(String[] org : origins){
            for(String[] dest: destinations){
                if(isValid(org, dest)){
                    double sum = calculateSum(org, dest);
                    context.write(new Text(""), new Text(String.format("%.0f",sum)));
                }
            }
        }

    }


    /**
     * determine if the two flights are not intersecting with each other
     * @param org String[] data of origin flight (Leg 1)
     * @param dest String[] data of destination flight (Leg 2)
     * @return true iff Leg1 flight is earlier than Leg2
     */
    private boolean isValid(String[] org, String[] dest) {
        return  Integer.parseInt(dest[INDEX_TIME]) - Integer.parseInt(org[INDEX_TIME]) > 0;
    }

    /**
     * calculate sum for delays
     * @param org String[] Leg1 flight data
     * @param dest String[] Leg2 flight data
     * @return Double sum of delays of flight
     */
    private double calculateSum(String[] org, String[] dest) {
        return Double.parseDouble(org[INDEX_DELAY]) + Double.parseDouble(dest[INDEX_DELAY]);
    }
}
