import comprators.FlightGrouper;
import comprators.KeyComparator;
import conts.TableConts;
import mappers.FlightMapper;
import model.Key;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import partationer.FlightPartitioner;
import reducers.FlightReducer;

import java.io.IOException;

/**
 * Created by jalpanranderi on 3/17/15.
 */
public class HCompute {

    private static final String YEAR =  "2008";
    public static final String TRUE = "0.00";


    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 1) {
            System.err.println("Usage: H-COMPUTE <out>");
            System.exit(2);
        }

        // Apply filter while retrieving data from HBase
        FilterList filterList = new FilterList(
                FilterList.Operator.MUST_PASS_ALL);
        SingleColumnValueFilter yearFilter = new SingleColumnValueFilter(
                TableConts.COLUMN_FAMILY.getBytes(), TableConts.COLUMN_YEAR.getBytes(),
                CompareFilter.CompareOp.EQUAL, YEAR.getBytes());
        filterList.addFilter(yearFilter);

        SingleColumnValueFilter cancelledFilter = new SingleColumnValueFilter(
                TableConts.COLUMN_FAMILY.getBytes(), TableConts.COLUMN_CANCELLED.getBytes(),
                CompareFilter.CompareOp.EQUAL, TRUE.getBytes());
        filterList.addFilter(cancelledFilter);

        SingleColumnValueFilter divertedFilter = new SingleColumnValueFilter(
                TableConts.COLUMN_FAMILY.getBytes(), TableConts.COLUMN_DIVERTED.getBytes(),
                CompareFilter.CompareOp.EQUAL, TRUE.getBytes());
        filterList.addFilter(divertedFilter);



        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        scan.setFilter(filterList);

        Job job = new Job(conf, "H-Compute");
        job.setJarByClass(HCompute.class);
        job.setMapperClass(FlightMapper.class);
        job.setReducerClass(FlightReducer.class);

        job.setOutputKeyClass(Key.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setPartitionerClass(FlightPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(FlightGrouper.class);

        job.setNumReduceTasks(12);


        TableMapReduceUtil.initTableMapperJob(TableConts.TABLE_NAME, scan,
                FlightMapper.class, Key.class, DoubleWritable.class, job);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[0]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
