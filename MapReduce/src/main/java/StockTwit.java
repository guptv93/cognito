import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.NullWritable;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

public class StockTwit {

     public static void main(String[] args) throws Exception {
            if (args.length != 1) {
                System.err.println("Usage: StockTwit <input path>");
                System.exit(-1);
            }
            Calendar cal = Calendar.getInstance();
            SimpleDateFormat sdfPartition = new SimpleDateFormat("yyyy-MM-dd'T'HH");
            sdfPartition.setTimeZone(TimeZone.getTimeZone("America/New_York"));
            String partitionTime = sdfPartition.format(cal.getTime());

            String outputPath = "/user/vvg239/cognito/newoutput/";
            Job job = new Job();
            job.setJarByClass(StockTwit.class);
            job.setJobName("Stock Twit Cleaning");
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(outputPath));
            job.setMapperClass(StockTwitMapper.class);
            job.setReducerClass(StockTwitReducer.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setNumReduceTasks(1);
            System.exit(job.waitForCompletion(true) ? 0 : 1);
     }
}
