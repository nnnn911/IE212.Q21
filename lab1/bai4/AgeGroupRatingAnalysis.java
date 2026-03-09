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

public class AgeGroupRatingAnalysis {

    private static final String[] AGE_GROUPS = {"0-18", "18-35", "35-50", "50+"};

    private static String getAgeGroup(int age) {
        if (age <= 18)       return "0-18";
        else if (age <= 35)  return "18-35";
        else if (age <= 50)  return "35-50";
        else                 return "50+";
    }

    public static class AgeGroupMapper extends Mapper<Object, Text, Text, Text> {

        private final Map<String, String> movieMap   = new HashMap<>();
        private final Map<String, Integer> userAgeMap = new HashMap<>();

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
                            if (parts.length >= 3) {
                                try {
                                    int age = Integer.parseInt(parts[2].trim());
                                    userAgeMap.put(parts[0].trim(), age);
                                } catch (NumberFormatException e) {
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

            String userId  = parts[0].trim();
            String movieId = parts[1].trim();
            String rating  = parts[2].trim();

            String  title = movieMap.get(movieId);
            Integer age   = userAgeMap.get(userId);

            if (title == null || age == null) return;

            String ageGroup = getAgeGroup(age);

            context.write(new Text(title), new Text(ageGroup + ":" + rating));
        }
    }

    public static class AgeGroupReducer extends Reducer<Text, Text, Text, Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Double> sumMap   = new HashMap<>();
            Map<String, Long>   countMap = new HashMap<>();

            for (String g : AGE_GROUPS) {
                sumMap.put(g, 0.0);
                countMap.put(g, 0L);
            }

            for (Text val : values) {
                String[] gv = val.toString().split(":", 2);
                if (gv.length < 2) continue;
                String group = gv[0].trim();
                double rating;
                try {
                    rating = Double.parseDouble(gv[1].trim());
                } catch (NumberFormatException e) {
                    continue;
                }
                if (sumMap.containsKey(group)) {
                    sumMap.put(group,   sumMap.get(group)   + rating);
                    countMap.put(group, countMap.get(group) + 1);
                }
            }

            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < AGE_GROUPS.length; i++) {
                String g = AGE_GROUPS[i];
                long   c = countMap.get(g);
                String avg = c > 0 ? String.format("%.2f", sumMap.get(g) / c) : "NA";
                sb.append(g).append(": ").append(avg);
                if (i < AGE_GROUPS.length - 1) sb.append(", ");
            }

            context.write(new Text(key.toString() + ":"), new Text(sb.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println("Usage: AgeGroupRatingAnalysis <movies_path> <users_path> <ratings1_path> <ratings2_path> <output_path>");
            System.exit(1);
        }

        String moviesPath   = args[0];
        String usersPath    = args[1];
        String ratings1Path = args[2];
        String ratings2Path = args[3];
        String outputPath   = args[4];

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Rating Analysis - Bai 4");

        job.setJarByClass(AgeGroupRatingAnalysis.class);
        job.setMapperClass(AgeGroupMapper.class);
        job.setReducerClass(AgeGroupReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.addCacheFile(new URI(moviesPath + "#movies.txt"));
        job.addCacheFile(new URI(usersPath  + "#users.txt"));

        MultipleInputs.addInputPath(job, new Path(ratings1Path), TextInputFormat.class, AgeGroupMapper.class);
        MultipleInputs.addInputPath(job, new Path(ratings2Path), TextInputFormat.class, AgeGroupMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
