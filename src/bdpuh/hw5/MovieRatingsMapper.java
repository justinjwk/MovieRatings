package bdpuh.hw5;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

public class MovieRatingsMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

    Logger logger = Logger.getLogger(MovieRatingsMapper.class);
    IntWritable movieId;

    String fileName;
    private final String DATA_SPLIT = "\t";
    private final String ITEM_SPLIT = "|";
    private final int RATING = 2;
    private final int USER_ID = 0;
    private final int MOVIE_TITLE = 1;
    private final int RELEASE_DATE = 2;
    private final int IMDB_URL = 4;
    private final String DATA_FILE_TAG = "data";
    private final String ITEM_FILE_TAG = "item";

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        logger.info("in setup of " + context.getTaskAttemptID().toString());
        fileName = ((FileSplit) context.getInputSplit()).getPath() + "";
        System.out.println ("in stdout"+ context.getTaskAttemptID().toString() + " " +  fileName);
        System.err.println ("in stderr"+ context.getTaskAttemptID().toString());
    }

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();

        // if the file is u#.data
        if (fileName.contains("data")) {
            String[] values = line.split(DATA_SPLIT);
            movieId.set(Integer.parseInt(values[1]));                         // value[1] -> movieID
            context.write(movieId, encodeDataFile(values));
        }
        // if the file is u.item
        else {
            String[] values = line.split(ITEM_SPLIT);
            movieId.set(Integer.parseInt(values[0]));                         // value[0] -> movieID
            context.write(movieId, encodeItemFile(values));
        }
    }

    protected Text encodeDataFile(String[] values) {

        String returnData = null;
        returnData = DATA_FILE_TAG + "\t" + values[RATING] + "\t" + values[USER_ID];

        return new Text(returnData);
    }

    protected Text encodeItemFile(String[] values) {

        String returnData = null;
        returnData = ITEM_FILE_TAG + "\t" + values[MOVIE_TITLE] + "\t" + values[RELEASE_DATE] + "\t" + values[IMDB_URL];

        return new Text(returnData);
    }

}
