
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Vector;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class KNNMapper extends Mapper<Text, SparseVector, Text, Vector2SF> {

	private Vector<Vector2<String, SparseVector>> train = new Vector<Vector2<String, SparseVector>>();

	protected void map(
			Text key,
			SparseVector value,
			org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, Text, Vector2SF>.Context context)
			throws java.io.IOException, InterruptedException {
		// calculate the distance for each test sample with the training and
		// sort
		context.setStatus(key.toString());
		ArrayList<Vector2<Vector2<String, SparseVector>, Double>> res = new ArrayList<Vector2<Vector2<String, SparseVector>, Double>>();
		for (Vector2<String, SparseVector> trainCase : train) {
			double d = trainCase.getV2().euclideanDistance(value);
			res.add(new Vector2<Vector2<String, SparseVector>, Double>(
					trainCase, d));
		}
		Collections
				.sort(res,
						new Comparator<Vector2<Vector2<String, SparseVector>, Double>>() {
							@Override
							public int compare(
									Vector2<Vector2<String, SparseVector>, Double> o1,
									Vector2<Vector2<String, SparseVector>, Double> o2) {
								return Double.compare(o2.getV2(), o1.getV2());
							}
						});
		HashMap<String, Integer> vs = new HashMap<String, Integer>();
		int k = context.getConfiguration().getInt("org.niubility.knn.k", 5);
		for (int i = 0; i < k; i++) {
			if (!vs.containsKey(res.get(i).getV1().getV1())) {
				vs.put(res.get(i).getV1().getV1(), 1);
			} else {
				int tmp = vs.get(res.get(i).getV1().getV1());
				vs.put(res.get(i).getV1().getV1(), ++tmp);
			}
		}

		int max = 0;
		String str = new String();
		for (Map.Entry<String, Integer> e : vs.entrySet()) {
			if (e.getValue() > max) {
				max = e.getValue();
				str = e.getKey();
			}
		}

		StringBuilder resStr = new StringBuilder();
		for (int i = 1; i <= value.size(); i++)
			resStr.append(value.get(i + "") + " ");
		//System.out.println("The String is " + resStr.toString());
		System.out.println("The String is " + str);
		
		context.write(new Text(str), new Vector2SF(resStr.toString(), 0.f));
	}

	protected void cleanup(
			org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, Text, Vector2SF>.Context context)
			throws java.io.IOException, InterruptedException {
		// test.close();
	}

	;

	protected void setup(
			org.apache.hadoop.mapreduce.Mapper<Text, SparseVector, Text, Vector2SF>.Context context)
			throws java.io.IOException, InterruptedException {
		System.out.print("loading shared comparison vectors...");

		// load the test vectors
		FileSystem fs = FileSystem.get(context.getConfiguration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(new Path(context.getConfiguration().get(
						"org.niubility.learning.test", "test.arff")))));// ????
		String line = br.readLine();
		int count = 0;
		while (line != null) {
			Vector2<String, SparseVector> v = ARFFInputformat.readLine(count,
					line);
			train.add(new Vector2<String, SparseVector>(v.getV1(), v.getV2()));
			line = br.readLine();
			count++;
		}
		br.close();
		System.out.println("done.");
	}

	;
}
