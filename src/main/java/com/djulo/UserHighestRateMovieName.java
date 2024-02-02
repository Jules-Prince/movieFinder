package com.djulo;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.djulo.mapper.MovieCountMapper;
import com.djulo.mapper.MovieIDMovieNameMapper;
import com.djulo.mapper.MovieIDUserIDMapper;
import com.djulo.mapper.RatingMapper;
import com.djulo.reducer.HighestRatedReducer;
import com.djulo.reducer.MovieCountReducer;
import com.djulo.reducer.UserCountMovieNameReducer;

public class UserHighestRateMovieName {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 3) {
            System.err.println("Usage: MovieMapper <input_rating_path> <input_movie_path> <output_path>");
            System.exit(-1);
        }

        Job job1 = findFavouriteMovieForEachUser(args);

        if (job1.waitForCompletion(true))
            System.out.println("JOB 1 IS DONE !");

        Job job2 = countNumberOfUserLikeEachMovie(args);

        if (job2.waitForCompletion(true))
            System.out.println("JOB 2 IS DONE !");

        Job job3 = mapAllMovieWithTheSameAmountOfPeopleThatLikeThem(args);

        if (job3.waitForCompletion(true))
            System.out.println("JOB 3 IS DONE !");
    }

    private static Job mapAllMovieWithTheSameAmountOfPeopleThatLikeThem(String[] args) throws IOException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "movie count");

        job.setJarByClass(UserHighestRateMovieName.class);

        job.setMapperClass(MovieCountMapper.class);
        job.setReducerClass(MovieCountReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[2] + "job2/part-r-00000"));
        TextOutputFormat.setOutputPath(job, new Path(args[2] + "job3/"));

        return job;
    }

    private static Job countNumberOfUserLikeEachMovie(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "User Count by Movie Name");
        job.setJarByClass(UserHighestRateMovieName.class);

        MultipleInputs.addInputPath(job, new Path(args[1]),
                TextInputFormat.class, MovieIDMovieNameMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2] + "job1/part-r-00000"),
                TextInputFormat.class, MovieIDUserIDMapper.class);

        job.setReducerClass(UserCountMovieNameReducer.class);
        job.setNumReduceTasks(1);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job, new Path(args[2] + "job2/"));

        return job;
    }

    private static Job findFavouriteMovieForEachUser(String[] args) throws IOException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "highestRatedMoviePerUser");
        job.setJarByClass(UserHighestRateMovieName.class);

        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(HighestRatedReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(RatingInfo.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[2] + "job1/"));

        return job;
    }
}
