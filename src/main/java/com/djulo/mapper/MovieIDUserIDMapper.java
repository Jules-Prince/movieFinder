package com.djulo.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class MovieIDUserIDMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text one = new Text("1");
    private Text movieID = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        if (fields.length >= 2) {
            movieID.set(fields[1]);
            context.write(movieID, one);
        }
    }
}