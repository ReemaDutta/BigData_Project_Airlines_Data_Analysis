package MapReduce.MR11_Perc_Carr_Delay;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class DelayCarrierMain {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{


        Configuration conf = new Configuration();
        // Create a new Job
        Job job = Job.getInstance(conf,"delay carrier");
        //job.setJarByClass(DelayCarrierMain.class);

        // Specify various job-specific parameters
        job.setJobName("myjob");


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DelayTuple.class);


        job.setMapperClass(DelayMapper.class);
        //job.setCombinerClass(DelayCarrierReducer.class);
        job.setReducerClass(DelayReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DelayTuple.class);

        job.waitForCompletion(true);

        ////////////////////////////////////////////////////

        Job job1 = new Job(conf, "Reduce-side join");

        job1.setJarByClass(DelayCarrierMain.class);
        job1.setReducerClass(InnerJoinReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, CarrierNameMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, FlightMapper.class);
        Path outputPath = new Path(args[3]);

        FileOutputFormat.setOutputPath(job1, outputPath);
        outputPath.getFileSystem(conf).delete(outputPath);
        //System.exit(job1.waitForCompletion(true) ? 0 : 1);

        // Submit the job, then poll for progress until the job is complete
        System.exit(job1.waitForCompletion(true)?0:1);

    }
}
