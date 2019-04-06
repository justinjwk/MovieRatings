package bdpuh.hw5;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingsReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    int i = 0;
    IntWritable count = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int rating = 0;
        String userID = null;
        String movieTitle = null;
        String releaseDate = null;
        String imdbURL = null;
        String fileType = null;

        String data;
        String[] splittedData;
        String result;

        for (Text value : values) {
            data = value.toString();
            splittedData = data.split("\t");

            fileType = splittedData[0];

            if (fileType.equals("data")) {
                rating = Integer.parseInt(splittedData[1]);
                userID = splittedData[2];
            }
            else if (fileType.equals("item")) {
                movieTitle = splittedData[1];
                releaseDate = splittedData[2];
                imdbURL = splittedData[3];
            }
        }


        result = movieTitle + "\t" + releaseDate + "\t" + imdbURL;

        context.write (key, new Text(result));
    }
}
