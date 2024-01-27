package com.djulo.genreCount;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenreCountMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text genre = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split(",");
        if(parts.length == 3){
            String[] genres = parts[2].split("\\|");
            for(String g : genres){
                Text outputKey = new Text(g.toUpperCase().trim());
                IntWritable outputValue = new IntWritable(1);
                context.write(outputKey, outputValue);
            }
        }
    }
}
