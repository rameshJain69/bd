package com.umermansoor;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import org.apache.hadoop.io.Text;



public class EarthquakeReducer extends 
        Reducer<Text, DoubleWritable, Text, DoubleWritable> 
{

    public void reduce(Text key, Iterable<DoubleWritable> values, 
            Context context) throws IOException, InterruptedException {
        
        // Standard algorithm for finding the max value
        double maxMagnitude = Double.MIN_VALUE;
        for (DoubleWritable var : values) {
            maxMagnitude = Math.max(maxMagnitude, var.get());
        }
        context.write(key, new DoubleWritable(maxMagnitude));
    }
}
