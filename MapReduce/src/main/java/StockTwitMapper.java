import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Arrays;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StockTwitMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Calendar cal = Calendar.getInstance();
        SimpleDateFormat sdfInput = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
        SimpleDateFormat sdfPartition = new SimpleDateFormat("yyyy-MM-dd'T'HH:00:00'Z'");
        SimpleDateFormat sdfOutput = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

        sdfInput.setTimeZone(TimeZone.getTimeZone("UTC"));
        sdfPartition.setTimeZone(TimeZone.getTimeZone("America/New_York"));
        sdfOutput.setTimeZone(TimeZone.getTimeZone("America/New_York"));

        String partitionTime = sdfPartition.format(cal.getTime());

        String line = value.toString();
        String[] splits = line.split("\\|\\|");
        String rawDate = splits[0];
        String twitTime = "";
        String createdAtET = "";
        try {
            twitTime = sdfPartition.format(sdfInput.parse(rawDate));
            createdAtET = sdfOutput.format(sdfInput.parse(rawDate));
        } catch(Exception e) {
            System.out.println(e);
        }


        if(!partitionTime.equals(twitTime)) return;
        String[] newSplits = Arrays.copyOfRange(splits, 1, splits.length);
        String formattedLine = createdAtET + "||" + String.join("||", newSplits);
        formattedLine = cleanTweet(formattedLine);
        String[] entities = StanfordNLP.applyNER(splits[3]);
        formattedLine = formattedLine + "||" + String.join(",", entities);
        context.write(new Text(partitionTime), new Text(formattedLine));
    }

    public String cleanTweet(String line) {
        final String sEmojiRegex = "(?:[\\u2700-\\u27bf]|" +

                "(?:[\\ud83c\\udde6-\\ud83c\\uddff]){2}|" +
                "[\\ud800\\udc00-\\uDBFF\\uDFFF]|[\\u2600-\\u26FF])[\\ufe0e\\ufe0f]?(?:[\\u0300-\\u036f\\ufe20-\\ufe23\\u20d0-\\u20f0]|[\\ud83c\\udffb-\\ud83c\\udfff])?" +

                "(?:\\u200d(?:[^\\ud800-\\udfff]|" +
                "(?:[\\ud83c\\udde6-\\ud83c\\uddff]){2}|" +
                "[\\ud800\\udc00-\\uDBFF\\uDFFF]|[\\u2600-\\u26FF])[\\ufe0e\\ufe0f]?(?:[\\u0300-\\u036f\\ufe20-\\ufe23\\u20d0-\\u20f0]|[\\ud83c\\udffb-\\ud83c\\udfff])?)*|" +

                "[\\u0023-\\u0039]\\ufe0f?\\u20e3|\\u3299|\\u3297|\\u303d|\\u3030|\\u24c2|[\\ud83c\\udd70-\\ud83c\\udd71]|[\\ud83c\\udd7e-\\ud83c\\udd7f]|\\ud83c\\udd8e|[\\ud83c\\udd91-\\ud83c\\udd9a]|[\\ud83c\\udde6-\\ud83c\\uddff]|[\\ud83c\\ude01-\\ud83c\\ude02]|\\ud83c\\ude1a|\\ud83c\\ude2f|[\\ud83c\\ude32-\\ud83c\\ude3a]|[\\ud83c\\ude50-\\ud83c\\ude51]|\\u203c|\\u2049|[\\u25aa-\\u25ab]|\\u25b6|\\u25c0|[\\u25fb-\\u25fe]|\\u00a9|\\u00ae|\\u2122|\\u2139|\\ud83c\\udc04|[\\u2600-\\u26FF]|\\u2b05|\\u2b06|\\u2b07|\\u2b1b|\\u2b1c|\\u2b50|\\u2b55|\\u231a|\\u231b|\\u2328|\\u23cf|[\\u23e9-\\u23f3]|[\\u23f8-\\u23fa]|\\ud83c\\udccf|\\u2934|\\u2935|[\\u2190-\\u21ff]";

        final Pattern pattern = Pattern.compile(sEmojiRegex);
        final Matcher matcher = pattern.matcher(line);

        //replace emojies
        line = matcher.replaceAll("");

        //remove links
        line = line.replaceAll("http:.*?\\s|https:.*?\\s", "");
        line = line.replaceAll("http:.*?\\||https:.*?\\|", "|");

        return line;
    }
}

