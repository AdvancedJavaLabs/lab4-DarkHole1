import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringReader;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ProductAnalyzer {
  public static class ProductResult implements Writable {
    private String category;
    private int quantity;
    private double revenue;

    ProductResult() {
    }

    ProductResult(String category, int quantity, double revenue) {
      this.category = category;
      this.quantity = quantity;
      this.revenue = revenue;
    }

    public void setCategory(String category) {
      this.category = category;
    }

    public void setQuantity(int quantity) {
      this.quantity = quantity;
    }

    public void setRevenue(double revenue) {
      this.revenue = revenue;
    }

    public void add(ProductResult other) {
      category = other.category;
      quantity += other.quantity;
      revenue += other.revenue;
    }

    public void write(DataOutput out) throws IOException {
      out.writeUTF(category);
      out.writeInt(quantity);
      out.writeDouble(revenue);
    }

    public void readFields(DataInput in) throws IOException {
      category = in.readUTF();
      quantity = in.readInt();
      revenue = in.readDouble();
    }

    public static ProductResult read(DataInput in) throws IOException {
      ProductResult res = new ProductResult();
      res.readFields(in);
      return res;
    }

    public String toString() {
      return String.format("%.2f\t%d", revenue, quantity);
    }
  }

  public static class ProductResultMapper
      extends Mapper<Object, Text, Text, ProductResult> {

    private ProductResult result = new ProductResult();
    private Text category = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      try {
        String[] values = value.toString().split(",");
        int transaction_id = Integer.parseInt(values[0]);
        int product_id = Integer.parseInt(values[1]);
        category.set(values[2]);
        double price = Double.parseDouble(values[3]);
        int quantity = Integer.parseInt(values[4]);

        result.setCategory(category.toString());
        result.setQuantity(quantity);
        result.setRevenue(price * quantity);
        context.write(category, result);
      } catch (Exception e) {
        // Ignore parsing errors
      }
    }
  }

  public static class KeySwapperMapper
      extends Mapper<Object, Text, DoubleWritable, ProductResult> {

    private ProductResult result = new ProductResult();
    private DoubleWritable revenue = new DoubleWritable();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      // try {
        String[] values = value.toString().split("\t");
        String category = values[0];
        double revenue = Double.parseDouble(values[1]);
        int quantity = Integer.parseInt(values[2]);

        result.setCategory(category);
        result.setQuantity(quantity);
        result.setRevenue(revenue);
        this.revenue.set(revenue);
        context.write(this.revenue, result);
      // } catch (Exception e) {
        // Ignore parsing errors
        // System.out.println("!!! error " + e);
      // }
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

  public static class SwapperReducer
      extends Reducer<DoubleWritable, ProductResult, Text, ProductResult> {
    public void reduce(Text key, Iterable<ProductResult> values,
        Context context) throws IOException, InterruptedException {
      Text category = new Text();
      for (ProductResult val : values) {
        category.set(val.category);
        context.write(category, val);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    long startTime = System.nanoTime();
    int splitSize = Integer.parseInt(args[3]);
    
    Configuration conf1 = new Configuration();
    conf1.setLong(FileInputFormat.SPLIT_MAXSIZE, splitSize * 1024 * 1024);
    Job job1 = Job.getInstance(conf1, "product analyzer");
    job1.setJarByClass(ProductAnalyzer.class);
    job1.setMapperClass(ProductResultMapper.class);
    job1.setCombinerClass(ProductResultReducer.class);
    job1.setReducerClass(ProductResultReducer.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(ProductResult.class);
    FileInputFormat.addInputPath(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, new Path(args[1]));
    job1.waitForCompletion(true);

    Configuration conf2 = new Configuration();
    conf2.setLong(FileInputFormat.SPLIT_MAXSIZE, splitSize * 1024 * 1024);
    Job job2 = Job.getInstance(conf2, "product sort");
    job2.setJarByClass(ProductAnalyzer.class);
    job2.setMapperClass(KeySwapperMapper.class);
    job2.setCombinerClass(SwapperReducer.class);
    job2.setReducerClass(SwapperReducer.class);
    job2.setOutputKeyClass(DoubleWritable.class);
    job2.setOutputValueClass(ProductResult.class);
    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    int res = job2.waitForCompletion(true) ? 0 : 1;
    
    long endTime = System.nanoTime();
    long duration = (endTime - startTime) / 1000000;
    System.out.printf("Took %d ms", duration);
    System.exit(res);
  }
}