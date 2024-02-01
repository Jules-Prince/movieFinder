package com.djulo.userHighestRateMovieName;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.djulo.highestRateMoviePerUser.HighestRatedMoviePerUser;
import com.djulo.userHighestRateMovieName.mapper.MovieCountMapper;
import com.djulo.userHighestRateMovieName.mapper.MovieIDMovieNameMapper;
import com.djulo.userHighestRateMovieName.mapper.MovieIDUserIDMapper;
import com.djulo.userHighestRateMovieName.mapper.RatingMapper;
import com.djulo.userHighestRateMovieName.reducer.HighestRatedReducer;
import com.djulo.userHighestRateMovieName.reducer.MovieCountReducer;
import com.djulo.userHighestRateMovieName.reducer.UserCountMovieName;

public class UserHighestRateMovieName {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        if (args.length != 3) {
            System.err.println("Usage: MovieMapper <input_rating_path> <input_movie_path> <output_path>");
            System.exit(-1);
        }

        Job job1 = findBestMovieForEachUser(args);

        if (job1.waitForCompletion(true)) 
            System.out.println("JOB 1 IS DONE !");
        

        Job job2 = countNumberOfUserLikeEachMovie(args);

        if (job2.waitForCompletion(true))
            System.out.println("JOB 2 IS DONE !");

        
        Job job3 = mapAllMovieWithTheSameAmountOfPeopleThatLikeThme(args);

        if (job3.waitForCompletion(true))
            System.out.println("JOB 3 IS DONE !");
    }

    private static Job mapAllMovieWithTheSameAmountOfPeopleThatLikeThme(String[] args) throws IOException {
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "movie count");

        job3.setJarByClass(UserHighestRateMovieName.class);

        job3.setMapperClass(MovieCountMapper.class);
        job3.setReducerClass(MovieCountReducer.class);

        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job3, new Path(args[2] + "job2/part-r-00000"));
        TextOutputFormat.setOutputPath(job3, new Path(args[2] + "job3/"));
        return job3;
    }

    private static Job countNumberOfUserLikeEachMovie(String[] args) throws IOException {
        Configuration conf2 = new Configuration();

        Job job2 = Job.getInstance(conf2, "User Count by Movie Name");
        job2.setJarByClass(UserHighestRateMovieName.class);

        MultipleInputs.addInputPath(job2, new Path(args[1]),
                TextInputFormat.class, MovieIDMovieNameMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[2] + "job1/part-r-00000"),
                TextInputFormat.class, MovieIDUserIDMapper.class);

        job2.setReducerClass(UserCountMovieName.class);
        job2.setNumReduceTasks(1);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        TextOutputFormat.setOutputPath(job2, new Path(args[2] + "job2/"));
        return job2;
    }

    private static Job findBestMovieForEachUser(String[] args) throws IOException {
        Configuration conf1 = new Configuration();

        Job job1 = Job.getInstance(conf1, "highestRatedMoviePerUser");
        job1.setJarByClass(HighestRatedMoviePerUser.class);

        job1.setMapperClass(RatingMapper.class);
        job1.setReducerClass(HighestRatedReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(RatingInfo.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        TextInputFormat.addInputPath(job1, new Path(args[0]));
        TextOutputFormat.setOutputPath(job1, new Path(args[2] + "job1/"));
        return job1;
    }
}
