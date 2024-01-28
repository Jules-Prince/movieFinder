package com.djulo.highestRateMoviePerUser;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class HighestRatedReducer extends Reducer<Text, RatingInfo, Text, RatingInfo> {

    @Override
    protected void reduce(Text key, Iterable<RatingInfo> values, Context context)
            throws IOException, InterruptedException {
        RatingInfo highestRated = null;

        for (RatingInfo value : values) {
            if (highestRated == null || value.getRating() > highestRated.getRating()) {
                highestRated = new RatingInfo(value.getMovieId(), value.getRating());
            }
        }

        if (highestRated != null) {
            context.write(key, highestRated);
        }
    }
}