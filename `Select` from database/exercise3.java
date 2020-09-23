import java.io.*;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class exercise3 extends Configured implements Tool {
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
        public void configure(JobConf job) {
        }

        public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
            String line = value.toString(); // Converts text object line to a java String.
            String comb_key;

            // Split with a regular expression
            String[] arrOfStr = line.split(",", -1);

            if (arrOfStr[165].matches("\\d{4}")) {
                int year = Integer.parseInt(arrOfStr[165]);
                if (year >= 2000 && year <= 2010){
                    comb_key = arrOfStr[0].trim() + ", " + arrOfStr[2].trim() + ", " + arrOfStr[3].trim();
                    output.collect(new Text(comb_key), new Text(""));
                }
            }
        }
    }



    public int run(String[] args) throws Exception {
        JobConf conf = new JobConf(getConf(), exercise3.class);
        conf.setJobName("exercise3");

        conf.setMapOutputKeyClass(Text.class);
        conf.setMapOutputValueClass(Text.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(Text.class);
        conf.setMapperClass(Map.class);
        conf.setNumReduceTasks(0);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);


        FileInputFormat.setInputPaths(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new exercise3(), args);
        System.exit(res);
    }
}