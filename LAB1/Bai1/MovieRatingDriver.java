import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MovieRatingDriver {

    // ─────────────────────────────────────────────────────────────────
    // MAPPER 1 — Đọc ratings_1.txt và ratings_2.txt
    // Input line: UserID,MovieID,Rating,Timestamp
    // Emit: key = MovieID, value = "R:<rating>"
    // ─────────────────────────────────────────────────────────────────
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");   // định dạng MovieLens 1M dùng ::
            if (parts.length < 3) return;

            String movieId = parts[1].trim();
            String rating  = parts[2].trim();

            // Tiền tố "R:" để Reducer biết đây là dữ liệu rating
            context.write(new Text(movieId), new Text("R:" + rating));
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // MAPPER 2 — Đọc movies.txt
    // Input line: MovieID,Title,Genres
    // Emit: key = MovieID, value = "M:<title>"
    // ─────────────────────────────────────────────────────────────────
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 2) return;

            String movieId = parts[0].trim();
            String title   = parts[1].trim();

            // Tiền tố "M:" để Reducer biết đây là tên phim
            context.write(new Text(movieId), new Text("M:" + title));
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // REDUCER
    // Input: key = MovieID, values = ["M:Title", "R:4", "R:5", ...]
    // ─────────────────────────────────────────────────────────────────
    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {

        // Biến lớp theo yêu cầu đề bài
        private String maxMovie  = "";
        private double maxRating = -1.0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String title      = null;
            double ratingSum  = 0.0;
            int    ratingCount = 0;

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("M:")) {
                    title = v.substring(2);          // lấy tên phim
                } else if (v.startsWith("R:")) {
                    try {
                        ratingSum += Double.parseDouble(v.substring(2));
                        ratingCount++;
                    } catch (NumberFormatException ignored) {}
                }
            }

            if (title == null || ratingCount == 0) return;

            double avg = ratingSum / ratingCount;
            String avgStr = String.format("%.2f", avg);

            // Xuất kết quả chính
            String outputVal = String.format(
                "AverageRating: %s (TotalRatings: %d)", avgStr, ratingCount);
            context.write(new Text(title), new Text(outputVal));

            // Cập nhật phim có điểm cao nhất (≥ 5 lượt)
            if (ratingCount >= 5 && avg > maxRating) {
                maxRating = avg;
                maxMovie  = title;
            }
        }

        // cleanup() — chạy sau khi tất cả reduce() kết thúc
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            if (!maxMovie.isEmpty()) {
                String msg = String.format(
                    "%s is the highest rated movie with an average rating of %.2f" +
                    " among movies with at least 5 ratings.",
                    maxMovie, maxRating);
                context.write(new Text("*** BEST MOVIE ***"), new Text(msg));
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // DRIVER — main()
    // ─────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println(
                "Usage: MovieRatingDriver <ratings1> <ratings2> <movies> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Average Rating");
        job.setJarByClass(MovieRatingDriver.class);

        // Dùng MultipleInputs vì mỗi loại file cần mapper riêng
        MultipleInputs.addInputPath(job, new Path(args[0]),
            TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
            TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]),
            TextInputFormat.class, MovieMapper.class);

        job.setReducerClass(RatingReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}