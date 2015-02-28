import mappers.AverageMapper;
import mappers.FlightMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import reducers.AverageReducer;
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

        job.setNumReduceTasks(10);

        job.setReducerClass(FlightReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path("temp"));


        job.waitForCompletion(true);


        Job job2 = new Job(conf, "Average");
        job2.setJarByClass(FlightDelayCalculator.class);
        job2.setMapperClass(AverageMapper.class);
        job2.setReducerClass(AverageReducer.class);
        job2.setNumReduceTasks(1);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path("temp"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));



        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }


}
