package com.djulo.reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.djulo.RatingInfo;

public class HighestRatedReducer extends Reducer<Text, RatingInfo, Text, Text> {
    
    protected void reduce(Text key, Iterable<RatingInfo> values, Context context)
            throws IOException, InterruptedException {
        RatingInfo highestRated = null;

        for (RatingInfo value : values) {
            if (highestRated == null || value.getRating() > highestRated.getRating()) {
                highestRated = new RatingInfo(value.getMovieId(), value.getRating());
            }
        }

        if (highestRated != null) {
            context.write(key, new Text(highestRated.getMovieId()));
        }
    }
}