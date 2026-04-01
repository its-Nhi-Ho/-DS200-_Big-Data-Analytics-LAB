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

public class AgeGroupRatingDriver {

    // ─────────────────────────────────────────────────────────────────
    // MAPPER 1 — Đọc ratings_1.txt và ratings_2.txt
    // Input : UserID, MovieID, Rating, Timestamp
    // Emit  : key = UserID  |  value = "R:<movieId>:<rating>"
    // ─────────────────────────────────────────────────────────────────
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

    // ─────────────────────────────────────────────────────────────────
    // MAPPER 2 — Đọc users.txt
    // Input : UserID, Gender, Age, Occupation, Zip
    // Emit  : key = UserID  |  value = "U:<ageGroup>"
    // ─────────────────────────────────────────────────────────────────
    public static class UserMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;

            String userId = parts[0].trim();
            // Age nằm ở cột index 2 (UserID, Gender, Age, Occupation, Zip)
            String ageStr = parts[2].trim();

            String ageGroup;
            try {
                int age = Integer.parseInt(ageStr);
                if      (age <= 18) ageGroup = "0-18";
                else if (age <= 35) ageGroup = "18-35";
                else if (age <= 50) ageGroup = "35-50";
                else                ageGroup = "50+";
            } catch (NumberFormatException e) {
                return; // Bỏ qua dòng không hợp lệ
            }

            context.write(new Text(userId), new Text("U:" + ageGroup));
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // REDUCER — Join theo UserID
    // movies.txt load từ DistributedCache trong setup()
    //
    // movieStats: movieId → double[8]
    //   [0] = sum_0_18,   [1] = count_0_18
    //   [2] = sum_18_35,  [3] = count_18_35
    //   [4] = sum_35_50,  [5] = count_35_50
    //   [6] = sum_50plus, [7] = count_50plus
    // ─────────────────────────────────────────────────────────────────
    public static class AgeGroupRatingReducer extends Reducer<Text, Text, Text, Text> {

        private final Map<String, String>   movieTitles = new HashMap<>();
        private final Map<String, double[]> movieStats  = new HashMap<>();

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

            System.err.println("[INFO] Loading movie titles from: " + moviePath);

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

            String ageGroup = null;
            List<String[]> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("U:")) {
                    ageGroup = v.substring(2);
                } else if (v.startsWith("R:")) {
                    String[] rParts = v.substring(2).split(":");
                    if (rParts.length == 2) ratings.add(rParts);
                }
            }

            if (ageGroup == null) return; // Không có thông tin user → bỏ qua

            for (String[] r : ratings) {
                String movieId = r[0];
                double rating;
                try {
                    rating = Double.parseDouble(r[1]);
                } catch (NumberFormatException e) {
                    continue;
                }

                movieStats.putIfAbsent(movieId, new double[8]);
                double[] stats = movieStats.get(movieId);

                switch (ageGroup) {
                    case "0-18":  stats[0] += rating; stats[1]++; break;
                    case "18-35": stats[2] += rating; stats[3]++; break;
                    case "35-50": stats[4] += rating; stats[5]++; break;
                    case "50+":   stats[6] += rating; stats[7]++; break;
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, double[]> entry : movieStats.entrySet()) {
                String   movieId = entry.getKey();
                double[] s       = entry.getValue();

                double avg0_18  = s[1] > 0 ? s[0] / s[1] : 0.0;
                double avg18_35 = s[3] > 0 ? s[2] / s[3] : 0.0;
                double avg35_50 = s[5] > 0 ? s[4] / s[5] : 0.0;
                double avg50p   = s[7] > 0 ? s[6] / s[7] : 0.0;

                String title = movieTitles.getOrDefault(movieId, "MovieID:" + movieId);

                String outputVal = String.format(
                    "[0-18: %.2f, 18-35: %.2f, 35-50: %.2f, 50+: %.2f]",
                    avg0_18, avg18_35, avg35_50, avg50p);

                context.write(new Text(title), new Text(outputVal));
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // DRIVER — main()
    // Args: <ratings1> <ratings2> <users> <movies> <output>
    // ─────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        if (args.length < 5) {
            System.err.println(
                "Usage: AgeGroupRatingDriver <ratings1> <ratings2> <users> <movies> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Group Movie Rating");
        job.setJarByClass(AgeGroupRatingDriver.class);

        job.addCacheFile(new URI(args[3] + "#movies.txt"));

        MultipleInputs.addInputPath(job, new Path(args[0]),
            TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
            TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]),
            TextInputFormat.class, UserMapper.class);

        job.setReducerClass(AgeGroupRatingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}