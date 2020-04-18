import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author Victoria Jenkins
 * Citations:
 * I used Activity 19 for reference to how to set up this mapper
 *
 */
public class KmerMapper extends Mapper<Object, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text kmer = new Text();
	
	@Override
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		int k = context.getConfiguration().getInt("k", 3);
		String line = value.toString();
		
		if (!line.startsWith(">")) {
			for (int i = 0; i < line.length(); i++) {
				if(i+k < line.length()) {
					String current = "";
					for(int j=0; j<k; j++) {
						current += line.charAt(j+i);
					}
					if(!current.contains("n")|| !current.contains("N")){
						kmer.set(current.toString());
						context.write(kmer, one);
					}				
					
				}
			}
		}
	}
}
