import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise4 extends Configured implements Tool {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
        public void configure(JobConf job) {
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            String line = value.toString(); // Converts text object line to a java String.

            // Split with a regular expression
            String[] arrOfStr = line.split(",", -1);
            output.collect(new Text(arrOfStr[2]), new DoubleWritable(Double.parseDouble(arrOfStr[3])));
        }
    }

    public static class myPartitioner implements Partitioner<Text, DoubleWritable> {
        public void configure(JobConf job) {
        }

        public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {

            String letter = key.toString().substring(0, 1).toUpperCase();


            if(letter.compareTo("F") < 0){
                return 0;//ABCDE
            } else if (letter.compareTo("K") < 0){
                return 1;//FGHIJ
            } else if (letter.compareTo("P") < 0){
                return 2;//KLMNO
            }else if (letter.compareTo("U") < 0){
                return 3;//PQRST
            } else {
                return 4;
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double dur, max_dur = 0.0;


            while (values.hasNext()) {
                dur = Double.parseDouble(values.next().toString());
                if(dur > max_dur){
                    max_dur = dur;
                }
            }
            output.collect(key, new DoubleWritable(max_dur));
        }
    }


        public int run(String[] args) throws Exception {
            JobConf conf = new JobConf(getConf(), exercise4.class);
            conf.setJobName("exercise4");

            //This line will explicitly set the number of reduce tasks.
            conf.setNumReduceTasks(5);
            //conf.setMapOutputKeyClass(Text.class);
            //conf.setMapOutputValueClass(DoubleWritable.class);
            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(DoubleWritable.class);

            conf.setMapperClass(Map.class);
            conf.setReducerClass(Reduce.class);
            conf.setPartitionerClass(myPartitioner.class);

            conf.setInputFormat(TextInputFormat.class);
            conf.setOutputFormat(TextOutputFormat.class);

            FileInputFormat.setInputPaths(conf, new Path(args[0]));
            FileOutputFormat.setOutputPath(conf, new Path(args[1]));

            JobClient.runJob(conf);
            return 0;
    }

        public static void main(String[] args) throws Exception {
            int res = ToolRunner.run(new Configuration(), new exercise4(), args);
            System.exit(res);
        }
}
