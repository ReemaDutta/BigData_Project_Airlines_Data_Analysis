package MapReduce.MR8_Carrier_Delay_Flight_Percentage;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class DelayCarrierMapper extends Mapper<Object, Text, Text, DelayCountTuple> {

    private DelayCountTuple tuple = new DelayCountTuple();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String [] tokens = value.toString().split(",");

        if(tokens[0].equals("Year"))return;

        String carrier = tokens[8];


        try {
            int delay = Integer.parseInt(tokens[14]);


            if (delay > 15) {
                tuple.setDelayedFlightCount(1);
            } else {
                tuple.setDelayedFlightCount(0);
            }
        }catch (Exception e){
            tuple.setDelayedFlightCount(0);
        }

        tuple.setFlightCount(1);

        context.write(new Text(carrier),tuple);
    }
}
