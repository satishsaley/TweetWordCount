package tweet.count;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TweetMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	public final static HashSet<String> stopWords = new HashSet<String>(Arrays.asList( "can't","u","i","&","just","rt","$$","-","ya","x","s","i'd","ur","said","did","hi","te","n","e","yo","having","1","2","3","4","5","6","7","8","9","0","la","em","o","es","en","lo","w","t","h", "fucking","bitch","a", "about", "above", "above",
			"across", "after", "afterwards", "again", "against", "all",
			"almost", "alone", "along", "already", "also", "although",
			"always", "am", "among", "amongst", "amoungst", "amount", "an",
			"and", "another", "any", "anyhow", "anyone", "anything", "anyway",
			"anywhere", "are", "around", "as", "at", "back", "be", "became",
			"because", "become", "becomes", "becoming", "been", "before",
			"beforehand", "behind", "being", "below", "beside", "besides",
			"between", "beyond", "both", "bottom", "but", "by", "call",
			"can", "cannot", "cant", "co", "com", "con", "could", "couldnt", "cry",
			"de", "describe", "detail", "do", "done", "down", "due", "during",
			"each", "eg", "eight", "either", "eleven", "else", "elsewhere",
			"empty", "enough", "etc", "even", "ever", "every", "everyone",
			"everything", "everywhere", "except", "few", "fifteen", "fify",
			"fill", "find", "fire", "first", "five", "for", "former",
			"formerly", "forty", "found", "four", "free", "from", "front", "full",
			"further", "get", "give", "go", "had", "has", "hasnt", "have",
			"he", "hence", "her", "here", "hereafter", "hereby", "herein",
			"hereupon", "hers", "herself", "him", "himself", "his", "how",
			"however", "hundred", "ie", "if", "in", "inc", "indeed",
			"interest", "into", "is", "it", "its", "itself", "keep", "last",
			"latter", "latterly", "least", "less", "ltd", "made", "many",
			"may", "me", "meanwhile", "might", "mill", "mine", "more",
			"moreover", "most", "mostly", "move", "much", "must", "my",
			"myself", "name", "namely", "neither", "net", "never", "nevertheless",
			"next", "nine", "no", "nobody", "none", "noone", "nor", "not",
			"nothing", "now", "nowhere", "of", "off", "often", "on", "once",
			"one", "only", "onto", "or", "org", "other", "others", "otherwise", "our",
			"ours", "ourselves", "out", "over", "own", "part", "per",
			"perhaps", "please", "put", "rather", "re", "same", "see", "seem",
			"seemed", "seeming", "seems", "serious", "several", "she",
			"should", "show", "side", "since", "sincere", "six", "sixty", "so",
			"some", "somehow", "someone", "something", "sometime", "sometimes",
			"somewhere", "still", "such", "system", "take", "ten", "than",
			"that", "the", "their", "them", "themselves", "then", "thence",
			"there", "thereafter", "thereby", "therefore", "therein",
			"thereupon", "these", "they", "thickv", "thin", "third", "this",
			"those", "though", "three", "through", "throughout", "thru",
			"thus", "to", "together", "too", "top", "toward", "towards",
			"twelve", "twenty", "two", "un", "under", "until", "up", "upon",
			"us", "very", "via", "was", "we", "well", "were", "what",
			"whatever", "when", "whence", "whenever", "where", "whereafter",
			"whereas", "whereby", "wherein", "whereupon", "wherever",
			"whether", "which", "while", "whither", "who", "whoever", "whole",
			"whom", "whose", "why", "will", "wikipedia", "with", "within", "without",
			"would", "yet", "you", "your", "yours", "yourself", "yourselves",
			"the","i","food",""," ","i'm","i'd","don't","do","it's", "...","where's", "who's","the...."));

	public static String URL_PATTERN = "^(https?|ftp|file|http)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]";

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
					throws IOException, InterruptedException {
		IntWritable one = new IntWritable(1);
		String line = value.toString();
		StringTokenizer st = new StringTokenizer(line);


		while(st.hasMoreTokens()){
			String currentWord = st.nextToken().toLowerCase().trim();
			if(stopWords.contains(currentWord))
				continue;
			if(currentWord.matches(URL_PATTERN))
				continue;
			if(currentWord.startsWith("@"))
				continue;
			if(currentWord.startsWith("#"))
				continue;
			//remove unnecessary marks
			//currentWord = trimWord(currentWord);
			
			if(currentWord.startsWith("http:"))
				continue;
			 currentWord=currentWord.replaceAll("((\\?*)|(\\.*)|(!*)|(,*)|(\"*)|(\\|*)|(\\'*))$", "");
			 currentWord=currentWord.replaceAll("(^( (\")|(\\|)|(\\!)|(\\')|(\\.)|(,) ))", "");
			if(!stopWords.contains(currentWord.trim())){
				Text t = new Text(currentWord);
				context.write(t, one);
			}
		}
	}

	private String trimWord(String currentOut) {

		String previousOut=currentOut;
		do
		{
			previousOut=currentOut;
			currentOut=(currentOut.replaceAll("((\\?*)|(\\.*)|(!*)|(,*)|(\"*)|(\\|*)|(\\'*))$", ""));
			currentOut=currentOut.replaceAll("(^( (\")|(\\|)|(\\!)|(\\')|(\\.)|(,) ))", "");
		}while(previousOut.equals(currentOut)==false);
	
		return currentOut;

	}

}
