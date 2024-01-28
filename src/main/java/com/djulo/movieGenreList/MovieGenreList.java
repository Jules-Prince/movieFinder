package com.djulo.movieGenreList;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class MovieGenreList {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        if (args.length != 3) {
            System.err.println("Usage: MovieGenreCount <input_path> <output_path> <genre>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("genre", args[2]);

        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = Job.getInstance(conf, "Movie List for a given genre");
        job.getConfiguration().set("genre", args[2]);
        
        job.setJarByClass(MovieGenreList.class);
        job.setMapperClass(MovieGenreListMapper.class);
        job.setReducerClass(MovieGenreListReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
