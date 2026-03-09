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

public class GenreRatingAnalysis {

    public static class GenreMapper extends Mapper<Object, Text, Text, Text> {

        private final Map<String, String> movieGenreMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null) {
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
                                if (parts.length >= 3) {
                                    String movieId = parts[0].trim();
                                    String genres  = parts[2].trim();
                                    movieGenreMap.put(movieId, genres);
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

            String genres = movieGenreMap.get(movieId);
            if (genres == null) return;

            for (String genre : genres.split("\\|")) {
                genre = genre.trim();
                if (!genre.isEmpty()) {
                    context.write(new Text(genre), new Text(rating));
                }
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {

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

            String output = String.format("Avg: %.2f,  Count: %d", avgRating, totalRatings);
            context.write(new Text(key.toString()), new Text(output));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: GenreRatingAnalysis <movies_path> <ratings1_path> <ratings2_path> <output_path>");
            System.exit(1);
        }

        String moviesPath   = args[0];
        String ratings1Path = args[1];
        String ratings2Path = args[2];
        String outputPath   = args[3];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Rating Analysis - Bai 2");

        job.setJarByClass(GenreRatingAnalysis.class);
        job.setMapperClass(GenreMapper.class);
        job.setReducerClass(GenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(moviesPath + "#movies.txt"));

        MultipleInputs.addInputPath(job, new Path(ratings1Path), TextInputFormat.class, GenreMapper.class);
        MultipleInputs.addInputPath(job, new Path(ratings2Path), TextInputFormat.class, GenreMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
