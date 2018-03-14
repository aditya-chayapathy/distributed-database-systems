import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Equijoin {

    public static void main(String[] args) throws Exception {
        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job j = new Job(c, "equijoin");

        j.setJarByClass(Equijoin.class);
        j.setMapperClass(EquijoinMapper.class);
        j.setReducerClass(EquijoinReducer.class);

        j.setMapOutputKeyClass(DoubleWritable.class);
        j.setMapOutputValueClass(Text.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class EquijoinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {
        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            String input = value.toString();
            String[] lines = input.split("\n");
            for (String line : lines) {
                String[] fields = line.split(", ");
                DoubleWritable outputKey = new DoubleWritable(Double.parseDouble(fields[1]));
                Text outputValue = new Text(line);
                con.write(outputKey, outputValue);
            }
        }
    }

    public static class EquijoinReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        public void reduce(DoubleWritable key, Iterable<Text> values, Context con) throws IOException, InterruptedException {
            List<String> lines = new ArrayList<String>();
            for (Text value : values) {
                lines.add(value.toString());
            }

            if (lines.size() > 1) {
                String table1 = "";
                String table2 = "";
                for (int i = 0; i < lines.size(); i++) {
                    String tableName = lines.get(i).split(", ")[0];
                    if (i == 0) {
                        table1 = tableName;
                        continue;
                    }
                    if (tableName.equals(table1)) {
                        table2 = tableName;
                        break;
                    }
                }

                List<String> table1Lines = new ArrayList<String>();
                List<String> table2Lines = new ArrayList<String>();
                for (String line : lines) {
                    String tableName = line.split(", ")[0];
                    if (tableName.equals(table1)) {
                        table1Lines.add(line);
                    } else {
                        table2Lines.add(line);
                    }
                }

                if (!table1Lines.isEmpty() && !table2Lines.isEmpty()) {
                    StringBuilder outputValue = new StringBuilder();
                    for (int i = 0; i < table1Lines.size(); i++) {
                        for (int j = 0; j < table2Lines.size(); j++) {
                            outputValue.append(table2Lines.get(j));
                            outputValue.append(", ");
                            outputValue.append(table1Lines.get(i));
                        }
                    }
                    con.write(null, new Text(outputValue.toString()));
                }
            }
        }
    }

}
