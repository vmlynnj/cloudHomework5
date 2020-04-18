import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class drives the program 
 * @author Victoria Jenkins
 * 
 * Citations:
 * https://my-bigdata-blog.blogspot.com/2017/07/custom-n-line-record-reader-in-hadoop.html
 * This article helped me figure out how to set the k variable in configurations
 * As well as how to point to the right input format classes.
 * I also used the Nucleotide Activity for reference to help set up the driver program
 *
 */

public class KmerDriver {

  public static void main(String[] args) throws Exception {


		if (args.length != 3) {
			System.err.println("Usage: Driver <in> <out> <k>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}

		Configuration conf = new Configuration();
		conf.setInt("k", Integer.parseInt(args[2]));
		Job job = Job.getInstance(conf, "Kmer");

		NLineInputFormat.setNumLinesPerSplit(job, 0);
		job.setJarByClass(KmerDriver.class);

		job.setMapperClass(KmerMapper.class);
		
		job.setCombinerClass(KmerReducer.class);
		job.setReducerClass(KmerReducer.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(KmerInputFormatNLines.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		NLineInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job,
				new Path(args[1] + System.currentTimeMillis()));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

