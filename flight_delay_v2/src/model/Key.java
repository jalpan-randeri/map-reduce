package model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * NameMonthPair servers as Key
 * <p/>
 * Created by jalpanranderi on 3/13/15.
 */
public class Key implements WritableComparable<Key> {
    private Text name;
    private IntWritable month;

    public Key() {
        name = new Text();
        month = new IntWritable();
    }

    public Key(String name, int date) {
        this.name = new Text(name);
        this.month = new IntWritable(date);
    }

    public String getName() {
        return name.toString();
    }


    public int getMonth() {
        return month.get();
    }

    @Override
    public String toString() {
        return getName() + ", " + getMonth();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Key)) return false;

        Key that = (Key) o;

        if (!month.equals(that.month)) return false;
        if (!name.equals(that.name)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + month.hashCode();
        return result;
    }

    @Override
    public int compareTo(Key o) {
        int cmp = name.compareTo(o.name);
        if (cmp == 0) {
            return month.compareTo(o.month);
        } else {
            return cmp;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        name.write(dataOutput);
        month.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        name.readFields(dataInput);
        month.readFields(dataInput);
    }
}
