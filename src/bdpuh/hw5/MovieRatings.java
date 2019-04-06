package bdpuh.hw5;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MovieRatings {
    public static void main(String args[])  {

//        String inputPath = args[0];
//        String outputPath = args[1];

//        String inputPath = "/movie-and-ratings";
//        String outputPath = "/movie-rating-result";

        String inputPath = "/in";
        String outputPath = "/out";

        Job movieRatingsJob = null;

        Configuration conf = new Configuration();

        //conf.setInt("mapred.reduce.tasks", 1);

        System.out.println ("======");
        try {
            movieRatingsJob = new Job(conf, "MovieRatings");
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        // Specify the Input path
        try {
            FileInputFormat.addInputPath(movieRatingsJob, new Path(inputPath));
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
            return;
        }

        // Set the Input Data Format
        movieRatingsJob.setInputFormatClass(TextInputFormat.class);

        // Set the Mapper and Reducer Class
        movieRatingsJob.setMapperClass(MovieRatingsMapper.class);
        movieRatingsJob.setReducerClass(MovieRatingsReducer.class);

        // Set the Jar file
        movieRatingsJob.setJarByClass(bdpuh.hw5.MovieRatings.class);

        // Set the Output path
        FileOutputFormat.setOutputPath(movieRatingsJob, new Path(outputPath));

        // Set the Output Data Format
        movieRatingsJob.setOutputFormatClass(TextOutputFormat.class);

        // Set the Output Key and Value Class
        movieRatingsJob.setOutputKeyClass(Text.class);
        movieRatingsJob.setOutputValueClass(IntWritable.class);

        // movieRatingsJob.setNumReduceTasks(5);

        try {
            movieRatingsJob.waitForCompletion(true);
        } catch (IOException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        } catch (InterruptedException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(MovieRatings.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
