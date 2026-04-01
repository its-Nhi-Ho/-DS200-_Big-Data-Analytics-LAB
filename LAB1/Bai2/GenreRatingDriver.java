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

public class GenreRatingDriver {

    // ─────────────────────────────────────────────────────────────────
    // MAPPER 1 — Đọc ratings_1.txt và ratings_2.txt
    // Input line : UserID, MovieID, Rating, Timestamp
    // Emit       : key = MovieID   |   value = "R:<rating>"
    // ─────────────────────────────────────────────────────────────────
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            String[] parts = line.split(",");
            if (parts.length < 3) return;

            String movieId = parts[1].trim();
            String rating  = parts[2].trim();

            // Tiền tố "R:" để Reducer biết đây là dữ liệu rating
            context.write(new Text(movieId), new Text("R:" + rating));
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // MAPPER 2 — Đọc movies.txt
    // Input line : MovieID, Title, Genre1|Genre2|...
    // Emit       : key = MovieID   |   value = "M:<genres>"
    // ─────────────────────────────────────────────────────────────────
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().trim();
            if (line.isEmpty()) return;

            // Tách tối đa 3 phần để giữ nguyên title có dấu phẩy
            String[] parts = line.split(",", 3);
            if (parts.length < 3) return;

            String movieId = parts[0].trim();
            String genres  = parts[2].trim();   // "Action|Sci-Fi|..."

            // Tiền tố "M:" để Reducer biết đây là dữ liệu thể loại
            context.write(new Text(movieId), new Text("M:" + genres));
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // REDUCER
    // Input  : key = MovieID
    //          values = ["M:Action|Sci-Fi", "R:4.0", "R:3.5", ...]
    //
    // Vấn đề nếu emit ngay trong reduce():
    //   → Mỗi phim emit 1 dòng per genre → Drama xuất hiện nhiều lần
    //   → Output bị trùng, không gộp được
    //
    // Fix: Dùng HashMap<genre, [sum, count]> tích lũy trong reduce(),
    //      đến cleanup() mới tính avg và xuất 1 dòng duy nhất per genre.
    // ─────────────────────────────────────────────────────────────────
    public static class GenreReducer extends Reducer<Text, Text, Text, Text> {

        // genre -> [ratingSum, ratingCount]
        private final Map<String, double[]> genreStats = new HashMap<>();

        // Thể loại có điểm trung bình cao nhất
        private String bestGenre     = "";
        private double bestAvgRating = -1.0;
        private int    bestCount     = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String genres      = null;
            double ratingSum   = 0.0;
            int    ratingCount = 0;

            for (Text val : values) {
                String v = val.toString();
                if (v.startsWith("M:")) {
                    genres = v.substring(2);            // "Action|Sci-Fi|..."
                } else if (v.startsWith("R:")) {
                    try {
                        ratingSum += Double.parseDouble(v.substring(2));
                        ratingCount++;
                    } catch (NumberFormatException ignored) {}
                }
            }

            // Bỏ qua nếu thiếu thông tin
            if (genres == null || ratingCount == 0) return;

            // Cộng dồn vào genreStats thay vì emit ngay
            // → nhiều phim cùng genre sẽ được gộp lại đúng cách
            for (String genre : genres.split("\\|")) {
                genre = genre.trim();
                genreStats.putIfAbsent(genre, new double[]{0.0, 0});
                double[] stats = genreStats.get(genre);
                stats[0] += ratingSum;    // cộng dồn tổng rating
                stats[1] += ratingCount;  // cộng dồn số lượt
            }
        }

        // cleanup() — chạy 1 lần sau khi tất cả reduce() xong.
        // Lúc này genreStats đã gộp đủ toàn bộ phim → tính avg và xuất output.
        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            for (Map.Entry<String, double[]> entry : genreStats.entrySet()) {
                String   genre = entry.getKey();
                double[] stats = entry.getValue();

                double avg   = stats[0] / stats[1];
                int    count = (int) stats[1];

                String outputVal = String.format(
                    "Average rating: %.2f (Total ratings: %d)", avg, count);
                context.write(new Text(genre), new Text(outputVal));

                // Theo dõi thể loại có điểm trung bình cao nhất
                if (avg > bestAvgRating) {
                    bestAvgRating = avg;
                    bestGenre     = genre;
                    bestCount     = count;
                }
            }

            // In thể loại có điểm cao nhất
            if (!bestGenre.isEmpty()) {
                String msg = String.format(
                    "\"%s\" is the highest rated genre with average rating %.2f" +
                    " over %d rating(s).",
                    bestGenre, bestAvgRating, bestCount);
                context.write(new Text("*** BEST GENRE ***"), new Text(msg));
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────
    // DRIVER — main()
    // Args: <ratings1> <ratings2> <movies> <o>
    // ─────────────────────────────────────────────────────────────────
    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println(
                "Usage: GenreRatingDriver <ratings1> <ratings2> <movies> <o>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Genre Average Rating");
        job.setJarByClass(GenreRatingDriver.class);

        // MultipleInputs: mỗi loại file dùng mapper riêng
        MultipleInputs.addInputPath(job, new Path(args[0]),
            TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]),
            TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[2]),
            TextInputFormat.class, MovieMapper.class);

        job.setReducerClass(GenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}