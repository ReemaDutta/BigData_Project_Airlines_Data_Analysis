package MapReduce.MR11_Perc_Carr_Delay;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DelayReducer extends Reducer<Text, DelayTuple, Text, DelayTuple> {

    private DelayTuple res= new DelayTuple();

    @Override
    protected void reduce(Text key, Iterable<DelayTuple> values, Context context) throws IOException, InterruptedException {

        int total=0;
        int delayedTotal=0;

        for(DelayTuple dt: values){
            total += dt.getFlightCount();
            delayedTotal +=dt.getDelayedFlightCount();
        }

        double percent = ((double)delayedTotal/total)*100;

        res.setDelayedFlightCount(delayedTotal);
        res.setFlightCount(total);
        res.setDelayPercent(percent);

        context.write(key,res);
    }
}
