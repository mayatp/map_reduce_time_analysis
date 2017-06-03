import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Date;
import java.text.SimpleDateFormat;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> { 
    private final IntWritable one = new IntWritable(1);
    private Text new_date = new Text();
    
    // Sample input
    // epoch_time [0];tweetId [1];tweet(hashtag contained)[2]; device [3]
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        //1: Find feature and partial value, splits the line into different strings
        
        String[] line = value.toString().split(";");
	// the try catches and ignores any errors, as in this case there was a tweet threated as a date.. (please, see the 'Error_before_adding_try' file )
	// the Date and SimpleDateFormat libraries are added, so we can format the epoch_time to a real date
	
	try{
        if (line.length == 4) {
		Date tweetsdate = new Date(Long.parseLong(line[0]));
 		SimpleDateFormat dateconv = new SimpleDateFormat("MM/dd/yyyy");
		String date_format = dateconv.format(tweetsdate).toString();      
        	new_date.set(date_format);
		
		context.write(new_date, one);        
		}
	}
	catch(Exception e){
	System.out.println(e);
	
	}

        
    }
}
