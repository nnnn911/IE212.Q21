import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class MovieRatingAnalysis {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        private final Map<String, String> movieMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                for (URI uri : cacheFiles) {
                    String localPath = uri.getPath();
                    String fileName = localPath.substring(localPath.lastIndexOf('/') + 1);
                    if (fileName.equals("movies.txt")) {
                        try (BufferedReader reader = new BufferedReader(new FileReader(fileName))) {
                            String line;
                            while ((line = reader.readLine()) != null) {
                                line = line.trim();
                                if (line.isEmpty()) continue;
                                String[] parts = line.split(",", 3);
                                if (parts.length >= 2) {
                                    String movieId = parts[0].trim();
                                    String title   = parts[1].trim();
                                    movieMap.put(movieId, title);
                                }
                            }
                        }
                    }
                }
            }
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;

            String movieId = parts[1].trim();
            String rating  = parts[2].trim();

            String title = movieMap.getOrDefault(movieId, "Movie_" + movieId);

            context.write(new Text(title), new Text(rating));
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {

        private String  maxMovie  = null;
        private double  maxRating = -1.0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long   totalRatings = 0;
            double sumRatings   = 0.0;

            for (Text val : values) {
                try {
                    sumRatings += Double.parseDouble(val.toString().trim());
                    totalRatings++;
                } catch (NumberFormatException e) {
                }
            }

            if (totalRatings == 0) return;

            double avgRating = sumRatings / totalRatings;

            if (totalRatings >= 5 && avgRating > maxRating) {
                maxRating = avgRating;
                maxMovie  = key.toString();
            }

            String output = String.format("AverageRating: %.2f (TotalRatings: %d)", avgRating, totalRatings);
            context.write(key, new Text(output));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (maxMovie != null) {
                String summary = String.format(
                        "%s is the highest rated movie with an average rating of %.2f"
                        + " among movies with at least 5 ratings.",
                        maxMovie, maxRating);
                context.write(new Text(""), new Text(summary));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: MovieRatingAnalysis <movies_path> <ratings1_path> <ratings2_path> <output_path>");
            System.exit(1);
        }

        String moviesPath   = args[0];
        String ratings1Path = args[1];
        String ratings2Path = args[2];
        String outputPath   = args[3];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Analysis - Bai 1");

        job.setJarByClass(MovieRatingAnalysis.class);
        job.setMapperClass(RatingMapper.class);
        job.setReducerClass(RatingReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(moviesPath + "#movies.txt"));

        MultipleInputs.addInputPath(job, new Path(ratings1Path), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(ratings2Path), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
