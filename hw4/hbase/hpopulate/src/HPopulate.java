import conts.TableConts;
import mappers.FlightMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;


/**
 * Created by jalpanranderi on 3/17/15.
 */
public class HPopulate {



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (args.length != 1) {
            System.err.println("Usage: HPopulate <input>");
            System.exit(1);
        }

        createHbaseTable(conf);


        Job job = new Job(conf, "HPopulate");
        job.setJarByClass(HPopulate.class);
        job.setMapperClass(FlightMapper.class);
        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        job.setOutputFormatClass(TableOutputFormat.class);

        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, TableConts.TABLE_NAME);
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }



    public static void createHbaseTable(Configuration conf) throws IOException{

        Configuration co = HBaseConfiguration.create(conf);
        HTableDescriptor hd = new HTableDescriptor(TableConts.TABLE_NAME);
        hd.addFamily(new HColumnDescriptor(TableConts.COLUMN_FAMILY));
        HBaseAdmin admin = new HBaseAdmin(co);

        if(admin.tableExists(TableConts.TABLE_NAME)){
            admin.disableTable(TableConts.TABLE_NAME);
            admin.deleteTable(TableConts.TABLE_NAME);
        }

        admin.createTable(hd);
        admin.close();
    }
}
