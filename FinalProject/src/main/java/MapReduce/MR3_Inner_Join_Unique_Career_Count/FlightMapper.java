package MapReduce.MR3_Inner_Join_Unique_Career_Count;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, Text, Text> {

    //    hadoop datatype
    Text word = new Text();
    IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] tokens= line.split("\t");
        if(tokens[0].equals("Code"))return;
        String newKey = tokens[0];
        word.set(newKey);
        String outValue= "B"+tokens[1];
        context.write(word,new Text(outValue));
    }

}