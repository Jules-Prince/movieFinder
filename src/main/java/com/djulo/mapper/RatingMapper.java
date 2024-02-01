package com.djulo.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.djulo.RatingInfo;

public class RatingMapper extends Mapper<Object, Text, Text, RatingInfo> {

    private Text userId = new Text();
    private RatingInfo ratingInfo = new RatingInfo();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        // Skip the header (first line)
        if (((LongWritable) key).get() == 0) {
            return;
        }

        String[] columns = value.toString().split(",");

        // Ensure that the column length is correct before parsing
        if (columns.length == 4) {
            try {
                userId.set(columns[0]);
                String movieId = columns[1];
                float rating = Float.parseFloat(columns[2]);

                ratingInfo = new RatingInfo(movieId, rating);
                context.write(userId, ratingInfo);
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