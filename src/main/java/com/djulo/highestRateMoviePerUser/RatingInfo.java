package com.djulo.highestRateMoviePerUser;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class RatingInfo implements Writable {
    
    private String movieId;
    private float rating;

    public RatingInfo() {
    }

    public RatingInfo(String movieId, float rating) {
        this.movieId = movieId;
        this.rating = rating;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(movieId);
        out.writeFloat(rating);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        movieId = in.readUTF();
        rating = in.readFloat();
    }

    public String getMovieId() {
        return movieId;
    }

    public float getRating() {
        return rating;
    }

    @Override
    public String toString() {
        return movieId + ":" + rating;
    }
}