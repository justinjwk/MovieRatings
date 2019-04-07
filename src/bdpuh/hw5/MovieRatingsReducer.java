package bdpuh.hw5;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MovieRatingsReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    int i = 0;
    IntWritable count = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        String movieID = key.toString();

        int rating = 0;
        int ratingTotal = 0;
        int ratingCounter = 0;

        String userID = null;
        Set<String> uniqueUsers = new HashSet<>();

        String movieTitle = null;
        String releaseDate = null;
        String imdbURL = null;
        String fileType = null;

        String data;
        String[] splittedData;
        String result;

        for (Text value : values) {

            data = value.toString();
            splittedData = data.split(MovieRatingsMapper.TAP_SPLIT);

            fileType = splittedData[0];

            if (fileType.equals("data")) {
                rating = Integer.parseInt(splittedData[1]);
                ratingTotal = ratingTotal + rating;
                ratingCounter++;
                userID = splittedData[2];
                uniqueUsers.add(userID);
            }
            else if (fileType.equals("item")) {
                if (splittedData.length >= 4) {
                    movieTitle = splittedData[1];
                    releaseDate = splittedData[2];
                    imdbURL = splittedData[3];
                }
                else {
                    context.getCounter("Error", "ItemReducer").increment(1);
                    System.out.printf("Size: %d, MovieID : %s, Value: %s\n", splittedData.length, movieID, data);
                }
            }
        }

        double averageRating = (double)ratingTotal / (double)ratingCounter;

        result = movieTitle + "\t" + releaseDate + "\t" + imdbURL + "\t" + averageRating + "\t" + uniqueUsers.size() + "\t" + ratingCounter;

        context.write (key, new Text(result));
    }
}
