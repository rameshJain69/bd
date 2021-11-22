public class ReducerClass extends Reducer<Text,FloatWritable,Text,Text>{
    public void reduce(Text key,Iterable<FloatWritable> valueList,Context con) throws Exception{
        float total;
        int ct;
        for(FloatWritable var: valueList){
            total+=var.get();
            ct++;
        }
        float avg=(float)total/ct;
        String out = "Total: "+total+"::"+"Average: "+avg;
        con.write(key,new Text(out));
    }
}