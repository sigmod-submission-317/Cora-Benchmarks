package manual.arithmetic;

import org.apache.spark.api.java.JavaRDD;

public class Equal {
	public static boolean equal(JavaRDD<Integer> data, int val){
		final int v = val;
		return (boolean) data.filter(a -> a != v).isEmpty();
	}
}