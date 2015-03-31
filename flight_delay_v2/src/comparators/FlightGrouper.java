package comparators;

import model.Key;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 * Created by jalpanranderi on 3/14/15.
 */
public class FlightGrouper extends WritableComparator {
    public FlightGrouper() {
        super(Key.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        Key pair1 = (Key) a;
        Key pair2 = (Key) b;

        // group on only airlines names
        return pair1.getName().compareTo(pair2.getName());
    }
}
