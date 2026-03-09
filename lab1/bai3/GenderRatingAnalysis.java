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

public class GenderRatingAnalysis {

    public static class GenderMapper extends Mapper<Object, Text, Text, Text> {

        private final Map<String, String> movieMap  = new HashMap<>();
        private final Map<String, String> userGenderMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null) return;

            for (URI uri : cacheFiles) {
                String localPath = uri.getPath();
                String fileName  = localPath.substring(localPath.lastIndexOf('/') + 1);

                if (fileName.equals("movies.txt")) {
                    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            line = line.trim();
                            if (line.isEmpty()) continue;
                            String[] parts = line.split(",", 3);
                            if (parts.length >= 2) {
                                movieMap.put(parts[0].trim(), parts[1].trim());
                            }
                        }
                    }
                } else if (fileName.equals("users.txt")) {
                    try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            line = line.trim();
                            if (line.isEmpty()) continue;
                            String[] parts = line.split(",");
                            if (parts.length >= 2) {
                                userGenderMap.put(parts[0].trim(), parts[1].trim());
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

            String userId  = parts[0].trim();
            String movieId = parts[1].trim();
            String rating  = parts[2].trim();

            String title  = movieMap.get(movieId);
            String gender = userGenderMap.get(userId);

            if (title == null || gender == null) return;

            context.write(new Text(title), new Text(gender + ":" + rating));
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            long   maleCount  = 0, femaleCount  = 0;
            double maleSum    = 0.0, femaleSum   = 0.0;

            for (Text val : values) {
                String[] gv = val.toString().split(":", 2);
                if (gv.length < 2) continue;
                String gender = gv[0].trim();
                double rating;
                try {
                    rating = Double.parseDouble(gv[1].trim());
                } catch (NumberFormatException e) {
                    continue;
                }

                if (gender.equalsIgnoreCase("M")) {
                    maleSum += rating;
                    maleCount++;
                } else if (gender.equalsIgnoreCase("F")) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            String maleAvg   = maleCount   > 0 ? String.format("%.2f", maleSum   / maleCount)   : "N/A";
            String femaleAvg = femaleCount > 0 ? String.format("%.2f", femaleSum / femaleCount) : "N/A";

            String output = String.format("Male: %s, Female: %s", maleAvg, femaleAvg);
            context.write(new Text(key.toString() + ":"), new Text(output));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: GenderRatingAnalysis <movies_path> <users_path> <ratings1_path> <ratings2_path> <output_path>");
            System.exit(1);
        }

        String moviesPath   = args[0];
        String usersPath    = args[1];
        String ratings1Path = args[2];
        String ratings2Path = args[3];
        String outputPath   = args[4];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Rating Analysis - Bai 3");

        job.setJarByClass(GenderRatingAnalysis.class);
        job.setMapperClass(GenderMapper.class);
        job.setReducerClass(GenderReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(moviesPath + "#movies.txt"));
        job.addCacheFile(new URI(usersPath  + "#users.txt"));

        MultipleInputs.addInputPath(job, new Path(ratings1Path), TextInputFormat.class, GenderMapper.class);
        MultipleInputs.addInputPath(job, new Path(ratings2Path), TextInputFormat.class, GenderMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
