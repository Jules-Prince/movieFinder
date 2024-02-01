package com.djulo.userHighestRateMovieName.mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieCountMapper extends Mapper<LongWritable, Text, IntWritable, Text> {


    private Text movieName = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t");

        if (parts.length >= 2) {
            int count = Integer.parseInt(parts[0]);
            movieName.set(parts[1]);
            context.write(new IntWritable(count), movieName);
        }
    }
}
