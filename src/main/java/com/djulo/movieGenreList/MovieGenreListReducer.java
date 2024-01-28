package com.djulo.movieGenreList;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieGenreListReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text genre, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        StringBuilder movieList = new StringBuilder();

        for (Text movieTitle : values) {
            movieList.append(movieTitle.toString()).append(", ");
        }

        if (movieList.length() > 2) {
            movieList.setLength(movieList.length() - 2);
        }

        context.write(genre, new Text(movieList.toString()));
    }
}
