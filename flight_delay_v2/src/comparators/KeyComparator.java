package comparators;

import model.Key;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Compare two keys
 * <p/>
 * Created by jalpanranderi on 3/14/15.
 */
public class KeyComparator extends WritableComparator {

    public KeyComparator() {
        super(Key.class, true);
    }


    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        Key pair1 = (Key) a;
        Key pair2 = (Key) b;

        int cmp = pair1.getName().compareTo(pair2.getName());
        if (cmp == 0) {
            return Integer.compare(pair1.getMonth(), pair2.getMonth());
        } else {
            return cmp;
        }
    }
}
