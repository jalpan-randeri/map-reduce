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
                    context.write(new Text("1"), new Text(String.format("%.0f",sum)));
                }
            }
        }

    }

    private boolean isValid(String[] org, String[] dest) {
        return  Integer.parseInt(dest[INDEX_TIME]) - Integer.parseInt(org[INDEX_TIME]) > 0;
    }

    private double calculateSum(String[] org, String[] dest) {
        return Double.parseDouble(org[INDEX_DELAY]) + Double.parseDouble(dest[INDEX_DELAY]);
    }
}
