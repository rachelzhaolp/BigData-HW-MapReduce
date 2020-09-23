import java.io.*;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise1 extends Configured implements Tool {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        //declare private substring array
        private final static String[] subStr = new String[]{"nu","chi","haw"};
        public void configure(JobConf job) {
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString(); // Converts text object line to a java String.
            String comb_key;

            // Split with a regular expression for whitespace
            String[] arrOfStr = line.split("\\s+", -1);
            // 1 gram
            if (arrOfStr.length == 4 && arrOfStr[1].matches("\\d{4}")) {
                for (String i :subStr){
                    if (arrOfStr[0].toLowerCase().contains(i)) {
                        comb_key = arrOfStr[1] +  "," + i;
                        output.collect(new Text(comb_key), new Text(arrOfStr[3]+", 1.0"));
                    }
                }
            }
            //2 gram
            if (arrOfStr.length == 5 && arrOfStr[2].matches("\\d{4}")) {
                for (int j = 0; j < 2; j++){
                    for (String i: subStr){
                        if (arrOfStr[j].toLowerCase().contains(i)) {
                            comb_key = arrOfStr[2] +  "," + i;
                            output.collect(new Text(comb_key), new Text(arrOfStr[4]+", 1.0"));
                        }
                    }
                }
            }
        }
    }

    public static class AvgCombiner extends MapReduceBase implements Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            double sum = 0.0, count = 0.0;

            //Aggregate the sum and counts because these can be further used to calculate the mean in reducer
            while (values.hasNext()) {
                Text pair = values.next();
                String[] tokens = pair.toString().split(",");
                count++;
                sum += Double.parseDouble(tokens[0]);
            }
            output.collect(key, new Text(Double.toString(sum)+", "+Double.toString(count)));
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, DoubleWritable> {

        public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, DoubleWritable> output, Reporter reporter) throws IOException {
            double sum = 0.0, count = 0.0, avg = 0.0;


            while (values.hasNext()) {
                Text pair = values.next();
                String[] tokens = pair.toString().split(",");
                count += Double.parseDouble(tokens[1]);
                sum += Double.parseDouble(tokens[0]);
            }
            avg = sum/count;
            output.collect(key, new DoubleWritable(avg));
        }
    }

    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), exercise1.class);
        conf.setJobName("exercise1");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(DoubleWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);
        conf.setCombinerClass(AvgCombiner.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new exercise1(), args);
        System.exit(res);
    }
}