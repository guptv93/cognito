import edu.stanford.nlp.pipeline.CoreDocument;
import edu.stanford.nlp.pipeline.CoreEntityMention;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StanfordNLP {

    public static String[] applyNER(String tweet) throws IOException {
        Properties props =new Properties();
        props.setProperty("annotators", "tokenize, ssplit,pos,lemma,ner");
      //  props.setProperty("ner.applyFineGrained", "false");

        List<String> result = new ArrayList<String>();

        StanfordCoreNLP pipeline = new StanfordCoreNLP(props);
        if (tweet != null && tweet.length() > 0) {
            CoreDocument doc = new CoreDocument(tweet);
            try {
                pipeline.annotate(doc);
                for (CoreEntityMention em : doc.entityMentions()) {
                    if(em.entityType().compareTo("PERSON")==0){
                        result.add(em.text());
                    }
                    if(em.entityType().compareTo("ORGANIZATION")==0){
                        result.add(em.text());
                    }
                }

            } catch (Exception e) {
                System.out.println("Handled Exception....." + e);
            }
        }
        return result.toArray(new String[result.size()]);
    }

    public static void main(String[] args) throws Exception {
        String line = cleanTweet("   \uD83D\uDE01  abcd efghijklmnopqrstuvwxyzhttpvaibhavguptav   http://www.breitbart.com/tech/2018/10/09/the-good-censor-leaked-google-briefing-admits-abandonment-of-free-speech-for-safety-and-civility");
        System.out.println(line);
    }

    public static String cleanTweet(String line) {

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
        line = line.replaceAll("http:.*?\\s|https:.*?\\s|http:.*?$|https:.*?$", "");

        return line;
    }

}