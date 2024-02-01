package com.djulo.userHighestRateMovieName.reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserCountMovieName extends Reducer<Text, Text, Text, Text> {

    private Map<String, String> movieInfoMap = new HashMap<>();

    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int count = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(":");
            if (parts.length >= 2) {
                if (parts[0].equals("info")) {
                    movieInfoMap.put(key.toString(), parts[1]);
                }
            } else {
                count += Integer.parseInt(val.toString());
            }
        }

        if (count > 0) {
            context.write(new Text(String.valueOf(count)), new Text(movieInfoMap.get(key.toString())));
        }
    }
}