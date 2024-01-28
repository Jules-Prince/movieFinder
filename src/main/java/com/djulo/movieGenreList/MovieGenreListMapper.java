package com.djulo.movieGenreList;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MovieGenreListMapper extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] columns = line.split(",");

        // Get the genre from the configuration
        String specifiedGenre = context.getConfiguration().get("genre");

        // Assuming genres are in the third column (index 2)
        if (columns.length > 2) {
            String[] genres = columns[2].split("\\|");

            for (String genre : genres) {
                if (genre.trim().equalsIgnoreCase(specifiedGenre)) {
                    Text outputKey = new Text(genre.trim());
                    Text outputValue = new Text(columns[1].trim()); // Movie title
                    context.write(outputKey, outputValue);
                }
            }
        }
    }
}