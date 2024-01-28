package com.djulo.averageScoreUserID;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class AverageScoreUser {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();

        String[] files = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path input = new Path(files[0]);
        Path output = new Path(files[1]);

        Job job = Job.getInstance(conf, "Average score per userID");
        job.setJarByClass(AverageScoreUser.class);
        job.setMapperClass(AverageScoreUserMapper.class);
        job.setReducerClass(AverageScoreUserReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
