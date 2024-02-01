package com.djulo.highestRateMoviePerUser;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.djulo.userHighestRateMovieName.RatingInfo;

public class HighestRatedMoviePerUser {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: HighestRatedMoviePerUser <input_path> <output_path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "highestRatedMoviePerUser");
        job.setJarByClass(HighestRatedMoviePerUser.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(HighestRatedReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RatingInfo.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
