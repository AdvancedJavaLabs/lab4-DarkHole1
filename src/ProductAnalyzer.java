import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.StringTokenizer;

import javax.naming.Context;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProductAnalyzer {
  public static class ProductResult implements Writable {
    private int quantity;
    private double revenue;

    ProductResult() {
    }

    ProductResult(int quantity, double revenue) {
      this.quantity = quantity;
      this.revenue = revenue;
    }

    public void setQuantity(int quantity) {
      this.quantity = quantity;
    }

    public void setRevenue(double revenue) {
      this.revenue = revenue;
    }

    public void add(ProductResult other) {
      quantity += other.quantity;
      revenue += other.revenue;
    }

    public void write(DataOutput out) throws IOException {
      out.writeInt(quantity);
      out.writeDouble(revenue);
    }

    public void readFields(DataInput in) throws IOException {
      quantity = in.readInt();
      revenue = in.readDouble();
    }

    public static ProductResult read(DataInput in) throws IOException {
      ProductResult res = new ProductResult();
      res.readFields(in);
      return res;
    }
  }

  public static class ProductResultMapper
      extends Mapper<Object, Text, Text, ProductResult> {

    private ProductResult result = new ProductResult();
    private Text category = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      BufferedReader bufReader = new BufferedReader(new StringReader(value.toString()));
      String line = null;
      bufReader.readLine(); // Skip first line
      while ((line = bufReader.readLine()) != null) {
        String[] values = line.split(",");
        int transaction_id = Integer.parseInt(values[0]);
        int product_id = Integer.parseInt(values[1]);
        category.set(values[1]);
        double price = Double.parseDouble(values[3]);
        int quantity = Integer.parseInt(values[4]);

        result.setQuantity(quantity);
        result.setRevenue(price * quantity);
        context.write(category, result);
      }
    }
  }

  public static class ProductResultReducer
      extends Reducer<Text, ProductResult, Text, ProductResult> {
    public void reduce(Text key, Iterable<ProductResult> values,
        Context context) throws IOException, InterruptedException {
      ProductResult result = new ProductResult();
      for (ProductResult val : values) {
        result.add(val);
      }
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "product analyzer");
    job.setJarByClass(ProductAnalyzer.class);
    job.setMapperClass(ProductResultMapper.class);
    job.setCombinerClass(ProductResultReducer.class);
    job.setReducerClass(ProductResultReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(ProductResult.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
