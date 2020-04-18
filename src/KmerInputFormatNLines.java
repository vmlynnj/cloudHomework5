import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

/**
 * 
 * @author Victoria Jenkins
 * Citations
 * the homework 5 lecture
 *
 */
public class KmerInputFormatNLines extends NLineInputFormat{


	@Override
	public RecordReader<LongWritable, Text> createRecordReader(InputSplit split,
			TaskAttemptContext context) throws IOException {
		KmerNLineRecordReader reader = new KmerNLineRecordReader();
		return reader;
	}
}
