import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class KNNReducer extends Reducer<Text, Vector2SF, String, Text> {

    protected void reduce(
            Text key,
            java.lang.Iterable<Vector2SF> value,
            org.apache.hadoop.mapreduce.Reducer<Text, Vector2SF, String, Text>.Context context)
            throws java.io.IOException, InterruptedException {
    	
        for (Vector2SF v : value)
        	context.write(v.getV1(), key);    
        
    }
}
