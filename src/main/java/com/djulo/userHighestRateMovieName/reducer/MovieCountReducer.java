package com.djulo.userHighestRateMovieName.reducer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieCountReducer extends Reducer<IntWritable, Text, Text, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder movieList = new StringBuilder();

        for (Text value : values) {
            movieList.append(value.toString()).append(", ");
        }

        String formattedMovieList = movieList.substring(0, movieList.length() - 2);

        context.write(new Text(key.toString() + " have like the following film(s) : "), new Text(formattedMovieList));
    }
    
}
