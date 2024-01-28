package com.djulo.averageScoreMovieID;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageScoreMovieMapper extends Mapper<Object, Text, Text, FloatWritable> {

    private Text movieId = new Text();
    private FloatWritable rating = new FloatWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String line = value.toString();
        String[] parts = line.split(",");

        if (parts.length == 4) {
            try {
                movieId.set(parts[1]);
                rating.set(Float.parseFloat(parts[2]));
                context.write(movieId, rating);
            } catch (NumberFormatException e) {
                // Handle the case where the rating column is not a valid float
                System.err.println("Skipping line with invalid rating: " + value.toString());
            }
        } else {
            // Handle the case where the line does not have the expected number of columns
            System.err.println("Skipping line with unexpected number of columns: " + value.toString());
        }
    }
}
