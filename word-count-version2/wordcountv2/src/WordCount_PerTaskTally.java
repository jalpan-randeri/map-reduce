/**
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  This code is taken from hadoop src directory of hadoop 1.2.1 source code 
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.HashMap;

public class WordCount_PerTaskTally {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] otherArgs = parser.getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "word count");
        job.setJarByClass(WordCount_PerTaskTally.class);
        job.setMapperClass(TokenizerMapper.class);

        // disable combiner

        // job.setCombinerClass(IntSumReducer.class);

        job.setPartitionerClass(WordPartitioner.class);
        job.setNumReduceTasks(5);

        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TokenizerMapper extends
            Mapper<Object, Text, Text, IntWritable> {

        private HashMap<String, Integer> hashMap;

        @Override
        protected void setup(
                Context context)
                throws IOException, InterruptedException {
            super.setup(context);

            hashMap = new HashMap<String, Integer>();
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] words = value.toString().split(" ");

            for (String s : words) {
                if (!s.isEmpty() && isValidWord(s)) {
                    if (hashMap.containsKey(s)) {
                        hashMap.put(s, hashMap.get(s) + 1);
                    } else {
                        hashMap.put(s, 1);
                    }
                }
            }

        }

        /**
         * check if string is valid word or not
         *
         * @param word String representing the word
         * @return Boolean true only iff word starts from m,n,o,p,q letters
         */
        private boolean isValidWord(String word) {
            char c = Character.toLowerCase(word.charAt(0));
            return c >= 'm' && c <= 'q';
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            super.cleanup(context);

            for (String w : hashMap.keySet()) {
                context.write(new Text(w), new IntWritable(hashMap.get(w)));
            }
        }

    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * WordPartitioner assign the reduce tasks to reducers
     *
     * @author jalpanranderi
     */
    public static class WordPartitioner extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            switch (Character.toLowerCase(key.charAt(0))) {
                case 'm':
                    return 0;
                case 'n':
                    return 1 % numPartitions;
                case 'o':
                    return 2 % numPartitions;
                case 'p':
                    return 3 % numPartitions;
                case 'q':
                    return 4 % numPartitions;
            }

            // unknown word came into the map should not happen
            throw new IllegalStateException();
        }

    }
}
