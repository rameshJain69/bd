package average;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapperClass extends
  Mapper<LongWritable, Text, Text, FloatWritable> {
 public void map(LongWritable key, Text empRecord, Context con)
   throws IOException, InterruptedException {
  String[] word = empRecord.toString().split("\\t");
  String sex = word[3];
  try {
   Float salary = Float.parseFloat(word[8]);
   con.write(new Text(sex), new FloatWritable(salary));
  } catch (Exception e) {
   e.printStackTrace();
  }
 }
}
