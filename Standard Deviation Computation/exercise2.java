import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise2 extends Configured implements Tool {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        public void configure(JobConf job) {
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString(); // Converts text object line to a java String.

            // Split with a regular expression for whitespace
            String[] arrOfStr = line.split("\\s+", -1);

            if (arrOfStr.length == 4) {
                output.collect(new Text("volume"), new Text(arrOfStr[3]+", 1.0"));
            } else if(arrOfStr.length == 5){
                output.collect(new Text("volume"), new Text(arrOfStr[4]+", 1.0"));
            }
        }
    }

    public static class sdCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            double sqr_sum = 0.0, sum = 0.0, count = 0.0;

            //Aggregate the sum and counts because these can be further used to calculate the mean in reducer
            while (values.hasNext()) {
                Text pair = values.next();
                String[] tokens = pair.toString().split(",");
                count++;
                sum += Double.parseDouble(tokens[0]);
                sqr_sum += Math.pow(Double.parseDouble(tokens[0]), 2);
            }
            output.collect(key, new Text(Double.toString(sqr_sum)+", " + Double.toString(sum)+", "+Double.toString(count)));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double sqr_sum = 0.0, sum = 0.0, count = 0.0, sd = 0.0;


            while (values.hasNext()) {
                Text pair = values.next();
                String[] tokens = pair.toString().split(",");
                sqr_sum += Double.parseDouble(tokens[0]);
                sum += Double.parseDouble(tokens[1]);
                count += Double.parseDouble(tokens[2]);
            }
            sd = Math. sqrt(sqr_sum/count - Math.pow(sum/count, 2));
            output.collect(key, new DoubleWritable(sd));
        }
    }


    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), exercise2.class);
        conf.setJobName("exercise2");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setCombinerClass(sdCombiner.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new exercise2(), args);
        System.exit(res);
    }
}
