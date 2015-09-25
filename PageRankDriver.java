import java.io.IOException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class PageRankDriver {
	public static void main(String[] args) 
		throws IOException, ClassNotFoundException, InterruptedException {

		if (args.length != 2) {
			System.err.println("Error: Please Specify the Input and Output Parameters");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJobName("Page Rank");
		job.setJarByClass(PageRankDriver.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(PageRankMapper.class);
		job.setReducerClass(PageRankReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}