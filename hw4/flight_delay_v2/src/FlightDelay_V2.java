import comparators.FlightGrouper;
import comparators.KeyComparator;
import mapper.FlightMapper;
import model.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import partitionar.FlightPartitioner;
import reducer.FlightReducer;

import java.io.IOException;

/**
 * Created by jalpanranderi on 3/13/15.
 */
public class FlightDelay_V2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] otherArgs = parser.getRemainingArgs();

        if (args.length != 2) {
            System.out.println("Usage FlightDelay_V2 <input> <output>");
            System.exit(-1);
        }

        Job job = new Job(conf, "Flight Delay v2");
        job.setJarByClass(FlightDelay_V2.class);

        job.setMapperClass(FlightMapper.class);
        job.setReducerClass(FlightReducer.class);

        // set partitioner and secondary sort comparators
        job.setPartitionerClass(FlightPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(FlightGrouper.class);

        job.setNumReduceTasks(12);
        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(DoubleWritable.class);


        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
