// Driver.java
public class driver
    {
        public static void main(String[] args){
            Configuration conf=new Configuration();
            Job job = Job.getInstance(conf,"Finding");
            job.setJarByClass(driver.class);
            job.setReducerClass(Reducer.class);
            job.setMapperClass(Mapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(FloatWritable.class);

            Path pathInput=new Path(arg[0]);
            Path pathOutputDir=new Path(arg[1]);

            FileInputFormat.addInputPath(job,pathInput);
            FileInputFormat.setOutputPath(job,pathOutputDir);

            System.exit(job.waitForCompletion(true)?0:1);
    }
 }
//--------------------------------------------------------------------------------------
//  Mapper.java
public class Mapper extends Mapper<LongWritable, Text,Text,FloatWritable>{
    public void map(LongWritable key,Text empRecord,Context con) throws Exception{
        String[] word= empRecord.toString().split("\\t");
        String sex=word[3];
        Float sal = Float.parseFloat(word[8]);
        con.write(new Text sex, new FloatWritable sal);
    }
}

// -------------------------------------------------------------------------------------

//Reducer.java

public class ReducerClass extends Reducer<Text, FloatWritable, Text,Text>{
    public void reduce(Text key, Iterable<FloatWritable> valueList, Context con) throws Exception{
            float total=0.0;
            int ct=0;
            for(FloatWritable var: valueList){
                ct++;
                total+=var.get();
            }
            float avg;
            avg=(float)total/ct;
            String out=total+avg;
            con.write(key,new Text(out));
    }
}
