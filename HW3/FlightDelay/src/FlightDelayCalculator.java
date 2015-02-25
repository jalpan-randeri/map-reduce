import mappers.FlightMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import reducers.FlightReducer;

/**
 * Created by jalpanranderi on 2/23/15.
 */
public class FlightDelayCalculator {
    public static void main(String[] args) throws Exception {




        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] otherArgs = parser.getRemainingArgs();


        if(args.length != 2){
            System.out.println("Usage FlightDelayCalculator <input> <output>");
            System.exit(2);
        }

        Job job = new Job(conf, "Flight delay calculator");
        job.setJarByClass(FlightDelayCalculator.class);
        job.setMapperClass(FlightMapper.class);

        // disable combiner

//        job.setCombinerClass(FlightReducer.class);
//        job.setPartitionerClass(WordPartitioner.class);
        job.setNumReduceTasks(5);

        job.setReducerClass(FlightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
