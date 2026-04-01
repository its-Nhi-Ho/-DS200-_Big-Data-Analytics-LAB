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
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;

public class GenderRatingDriver {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
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
            context.write(new Text(userId), new Text("R:" + movieId + ":" + rating));
        }
    }

    public static class UserMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(",");
            if (parts.length < 2) return;
            String userId = parts[0].trim();
            String gender = parts[1].trim();
            context.write(new Text(userId), new Text("U:" + gender));
        }
    }

    public static class GenderRatingReducer extends Reducer<Text, Text, Text, Text> {

        private final Map<String, String>   movieTitles = new HashMap<>();
        private final Map<String, double[]> movieStats  = new HashMap<>();
        private String biggestGapTitle = "";
        private double biggestGap      = -1.0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                System.err.println("[WARN] No cache files found!");
                return;
            }

            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(cacheFiles[0], conf);
            Path moviePath = new Path(cacheFiles[0]);

            System.err.println("[INFO] Loading movie titles from HDFS: " + moviePath);

            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(fs.open(moviePath)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    line = line.trim();
                    if (line.isEmpty()) continue;
                    String[] parts = line.split(",", 3);
                    if (parts.length < 2) continue;
                    movieTitles.put(parts[0].trim(), parts[1].trim());
                }
            }
            System.err.println("[INFO] Loaded " + movieTitles.size() + " movie titles.");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String gender = null;
            List<String[]> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("U:")) {
                    gender = v.substring(2);
                } else if (v.startsWith("R:")) {
                    String[] rParts = v.substring(2).split(":");
                    if (rParts.length == 2) ratings.add(rParts);
                }
            }

            if (gender == null) return; // Không có thông tin user → bỏ qua

            for (String[] r : ratings) {
                String movieId = r[0];
                double rating;
                try {
                    rating = Double.parseDouble(r[1]);
                } catch (NumberFormatException e) {
                    continue;
                }

                movieStats.putIfAbsent(movieId, new double[]{0, 0, 0, 0});
                double[] stats = movieStats.get(movieId);

                if (gender.equalsIgnoreCase("M")) {
                    stats[0] += rating; stats[1]++;
                } else if (gender.equalsIgnoreCase("F")) {
                    stats[2] += rating; stats[3]++;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, double[]> entry : movieStats.entrySet()) {
                String   movieId = entry.getKey();
                double[] stats   = entry.getValue();

                double maleAvg   = stats[1] > 0 ? stats[0] / stats[1] : 0.0;
                double femaleAvg = stats[3] > 0 ? stats[2] / stats[3] : 0.0;

                String title = movieTitles.getOrDefault(movieId, "MovieID:" + movieId);

                String outputVal = String.format(
                    "Male_Avg: %.2f (count: %d), Female_Avg: %.2f (count: %d)",
                    maleAvg, (int) stats[1], femaleAvg, (int) stats[3]);

                context.write(new Text(title), new Text(outputVal));

                if (stats[1] > 0 && stats[3] > 0) {
                    double gap = Math.abs(maleAvg - femaleAvg);
                    if (gap > biggestGap) {
                        biggestGap      = gap;
                        biggestGapTitle = title;
                    }
                }
            }

            if (!biggestGapTitle.isEmpty()) {
                context.write(
                    new Text("*** BIGGEST GENDER GAP ***"),
                    new Text(String.format("\"%s\" gap=%.2f", biggestGapTitle, biggestGap)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println(
                "Usage: GenderRatingDriver <ratings1> <ratings2> <users> <movies> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender-based Movie Rating");
        job.setJarByClass(GenderRatingDriver.class);

        job.addCacheFile(new URI(args[3] + "#movies.txt"));

        MultipleInputs.addInputPath(job, new Path(args[0]),
            TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
            TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]),
            TextInputFormat.class, UserMapper.class);

        job.setReducerClass(GenderRatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}