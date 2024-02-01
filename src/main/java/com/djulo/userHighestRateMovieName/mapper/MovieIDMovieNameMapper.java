package com.djulo.userHighestRateMovieName.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.djulo.userHighestRateMovieName.reducer.UserCountMovieName;

public class MovieIDMovieNameMapper extends Mapper<Object, Text, Text, Text> {

    private Text movieID = new Text();
    private Text movieInfo = new Text();

    public static final Logger logger = Logger.getLogger(UserCountMovieName.class);

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(",");

        if (fields.length >= 2) {
            movieID.set(fields[0]);
            movieInfo.set("info:" + fields[1]); 
            context.write(movieID, movieInfo);
        }
    }
}
