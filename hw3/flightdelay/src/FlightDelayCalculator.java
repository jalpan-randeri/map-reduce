import mappers.AverageMapper;
import mappers.FlightMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import reducers.AverageReducer;
import reducers.FlightReducer;

import java.io.IOException;

/**
 * Created by jalpanranderi on 2/23/15.
 */
public class FlightDelayCalculator {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] otherArgs = parser.getRemainingArgs();

        if (args.length != 2) {
            System.out.println("Usage FlightDelayCalculator <input> <output>");
            System.exit(2);
        }


        Job job = createJob(conf, "Flight delay", FlightDelayCalculator.class, FlightMapper.class,
                            FlightReducer.class, 10, Text.class, Text.class, otherArgs[0], "temp");
        job.waitForCompletion(true);

        Job job2 = createJob(conf, "Average", FlightDelayCalculator.class, AverageMapper.class,
                            AverageReducer.class, 1, Text.class, Text.class, "temp", otherArgs[1]);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * job creator
     * @param conf Configuration
     * @param jobName String representing job name
     * @param jarClass Class main class
     * @param mapper Class Mapper class
     * @param reducer Class Reducer class
     * @param numReduceTask Integer total number of reducers
     * @param outputKey Class type for output keys
     * @param outValue  Class type for output values
     * @param inputPath String input path
     * @param outputPath String output path
     * @return Job object
     * @throws IOException
     */
    private static Job createJob(Configuration conf, String jobName, Class jarClass, Class<? extends Mapper> mapper,
                                 Class<? extends Reducer> reducer, int numReduceTask, Class outputKey, Class outValue,
                                 String inputPath, String outputPath) throws IOException {

        Job job = new Job(conf, jobName);
        job.setJarByClass(jarClass);
        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        job.setNumReduceTasks(numReduceTask);
        job.setOutputKeyClass(outputKey);
        job.setOutputValueClass(outValue);
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }
}
