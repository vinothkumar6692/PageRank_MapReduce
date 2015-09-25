import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer 
	extends Reducer<Text, Text, Text, Text>{
	@Override

	public void reduce(Text key, Iterable<Text> values, Context context) 
		throws IOException, InterruptedException{
		
		String outlinks = "";
		String inputValue = "";
		float finalPageRank = 0;
		String delimiter = ",";
		
		for (Text value : values) {
			inputValue = value.toString();
			if (inputValue.contains(delimiter)) {
				finalPageRank += Float.parseFloat(inputValue.split(delimiter)[1]);
			} else {
				outlinks = inputValue;
			}
		}
		
		context.write(key, new Text(outlinks + finalPageRank));
	}
	
}