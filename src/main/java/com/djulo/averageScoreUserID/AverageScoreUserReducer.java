package com.djulo.averageScoreUserID;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageScoreUserReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    private FloatWritable result = new FloatWritable();

    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
            throws IOException, InterruptedException {
        float sum = 0;
        int count = 0;

        for (FloatWritable value : values) {
            sum += value.get();
            count++;
        }

        if (count > 0) {
            result.set(sum / count);
            context.write(key, result);
        }
    }
}
