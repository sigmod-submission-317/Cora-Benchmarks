package tpch.util;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import tpch.schema.LineItem;
import tpch.schema.Part;
import tpch.schema.Supplier;
import tpch.translated.Query1;
import tpch.translated.Query15;
import tpch.translated.Query17;
import tpch.translated.Query6;

import java.text.ParseException;
import java.text.SimpleDateFormat;

public class Driver {
  static StructType schema_supp = new StructType(
          new StructField[] {
                  DataTypes.createStructField("s_suppkey", DataTypes.IntegerType, true),
                  DataTypes.createStructField("s_name", DataTypes.StringType, true),
                  DataTypes.createStructField("s_address", DataTypes.StringType, true),
                  DataTypes.createStructField("s_nationkey", DataTypes.IntegerType, true),
                  DataTypes.createStructField("s_phone", DataTypes.StringType, true),
                  DataTypes.createStructField("s_acctbal", DataTypes.DoubleType, true),
                  DataTypes.createStructField("s_comment", DataTypes.StringType, true)
          }
  );

  static StructType schema_part = new StructType(
          new StructField[] {
                  DataTypes.createStructField("p_partkey", DataTypes.IntegerType, true),
                  DataTypes.createStructField("p_name", DataTypes.StringType, true),
                  DataTypes.createStructField("p_mfgr", DataTypes.StringType, true),
                  DataTypes.createStructField("p_brand", DataTypes.StringType, true),
                  DataTypes.createStructField("p_type", DataTypes.StringType, true),
                  DataTypes.createStructField("p_size", DataTypes.IntegerType, true),
                  DataTypes.createStructField("p_container", DataTypes.StringType, true),
                  DataTypes.createStructField("p_retailprice", DataTypes.DoubleType, true),
                  DataTypes.createStructField("p_comment", DataTypes.StringType, true)
          }
  );

  static StructType schema_lineitem = new StructType(
          new StructField[] {
                  DataTypes.createStructField("l_orderkey", DataTypes.IntegerType, true),
                  DataTypes.createStructField("l_partkey", DataTypes.IntegerType, true),
                  DataTypes.createStructField("l_suppkey", DataTypes.IntegerType, true),
                  DataTypes.createStructField("l_linenumber", DataTypes.IntegerType, true),
                  DataTypes.createStructField("l_quantity", DataTypes.DoubleType, true),
                  DataTypes.createStructField("l_extendedprice", DataTypes.DoubleType, true),
                  DataTypes.createStructField("l_discount", DataTypes.DoubleType, true),
                  DataTypes.createStructField("l_tax", DataTypes.DoubleType, true),
                  DataTypes.createStructField("l_returnflag", DataTypes.StringType, true),
                  DataTypes.createStructField("l_linestatus", DataTypes.StringType, true),
                  DataTypes.createStructField("l_shipdate", DataTypes.DateType, true),
                  DataTypes.createStructField("l_commitdate", DataTypes.DateType, true),
                  DataTypes.createStructField("l_receiptdate", DataTypes.DateType, true),
                  DataTypes.createStructField("l_shipinstruct", DataTypes.StringType, true),
                  DataTypes.createStructField("l_shipmode", DataTypes.StringType, true),
                  DataTypes.createStructField("l_comment", DataTypes.StringType, true)
          }
  );

  static SparkSession spark;
  static JavaSparkContext sparkContext;

  static Dataset<Row> lineitem_sql;
  static Dataset<Row> part_sql;
  static Dataset<Row> supplier_sql;
  static JavaRDD<LineItem> lineitem;
  static JavaRDD<Part> part;
  static JavaRDD<Supplier> supplier;

  public static void main(String[] args) throws ParseException {
    // Create spark session
    if(args[3].startsWith("sql")) {
      spark = SparkSession
              .builder()
              .appName("TPC-H Testing")
              .getOrCreate();

      lineitem_sql = spark.read().option("delimiter","|").schema(schema_lineitem).csv(args[0]);
      part_sql = spark.read().option("delimiter","|").schema(schema_part).csv(args[1]);
      supplier_sql = spark.read().option("delimiter","|").schema(schema_supp).csv(args[2]);
    }
    else {
      sparkContext = new JavaSparkContext(new SparkConf().setAppName("TPC-H Testing"));

      final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      lineitem = sparkContext.hadoopFile(args[0], TextInputFormat.class, LongWritable.class, Text.class)
        .map(row -> {
          String[] tokens = row._2().toString().split("\\|");
          LineItem l = new LineItem();
          l.l_orderkey = Integer.parseInt(tokens[0]);
          l.l_partkey = Integer.parseInt(tokens[1]);
          l.l_suppkey = Integer.parseInt(tokens[2]);
          l.l_linenubmer = Integer.parseInt(tokens[3]);
          l.l_quantity = Double.parseDouble(tokens[4]);
          l.l_extendedprice = Double.parseDouble(tokens[5]);
          l.l_discount = Double.parseDouble(tokens[6]);
          l.l_tax = Double.parseDouble(tokens[7]);
          l.l_returnflag = tokens[8].charAt(0);
          l.l_linestatus = tokens[9].charAt(0);
          l.l_shipdate = df.parse(tokens[10]);
          l.l_commitdate = df.parse(tokens[11]);
          l.l_receiptdate = df.parse(tokens[12]);
          l.l_shipinstruct = tokens[13];
          l.l_shipmode = tokens[14];
          l.l_comment = tokens[15];
          return l;
        });

      part = sparkContext.hadoopFile(args[1], TextInputFormat.class, LongWritable.class, Text.class)
        .map( row -> {
          String[] tokens = row._2().toString().split("\\|");
          Part p = new Part();
          p.p_partkey = Integer.parseInt(tokens[0]);
          p.p_name = tokens[1];
          p.p_mfgr = tokens[2];
          p.p_brand = tokens[3];
          p.p_type = tokens[4];
          p.p_size = Integer.parseInt(tokens[5]);
          p.p_container = tokens[6];
          p.p_retailprice = Double.parseDouble(tokens[7]);
          p.p_comment = tokens[8];
          return p;
        });

      supplier = sparkContext.hadoopFile(args[2], TextInputFormat.class, LongWritable.class, Text.class)
        .map(row -> {
          String[] tokens = row._2().toString().split("\\|");
          Supplier s = new Supplier();
          s.s_suppkey = Integer.parseInt(tokens[0]);
          s.s_name = tokens[1];
          s.s_address = tokens[2];
          s.s_nationkey = Integer.parseInt(tokens[3]);
          s.s_phone = tokens[4];
          s.s_acctbal = Double.parseDouble(tokens[5]);
          s.s_comment = tokens[6];
          return s;
        });
    }

    if (args[3].equals("sql1")){
      long startTime = System.nanoTime();
      runSQL1();
      long endTime = System.nanoTime();
      System.out.println((endTime - startTime)/1000000000.0);
    }
    else if (args[3].equals("sql6")){
      long startTime = System.nanoTime();
      runSQL6();
      long endTime = System.nanoTime();
      System.out.println((endTime - startTime)/1000000000.0);
    }
    else if (args[3].equals("sql15")){
      long startTime = System.nanoTime();
      runSQL15();
      long endTime = System.nanoTime();
      System.out.println((endTime - startTime)/1000000000.0);
    }
    else if (args[3].equals("sql17")){
      long startTime = System.nanoTime();
      runSQL17();
      long endTime = System.nanoTime();
      System.out.println((endTime - startTime)/1000000000.0);
    }
    else if (args[3].equals("q1")){
      long startTime = System.nanoTime();
      runQ1();
      long endTime = System.nanoTime();
      System.out.println((endTime - startTime)/1000000000.0);
    }
    else if (args[3].equals("q6")){
      long startTime = System.nanoTime();
      runQ6();
      long endTime = System.nanoTime();
      System.out.println((endTime - startTime)/1000000000.0);
    }
    else if (args[3].equals("q15")){
      long startTime = System.nanoTime();
      runQ15();
      long endTime = System.nanoTime();
      System.out.println((endTime - startTime)/1000000000.0);
    }
    else if (args[3].equals("q17")){
      long startTime = System.nanoTime();
      runQ17();
      long endTime = System.nanoTime();
      System.out.println((endTime - startTime)/1000000000.0);
    }
    else if (args[3].equals("allsql")){
      long startTime = System.nanoTime();
      runSQL1();
      runSQL6();
      runSQL15();
      runSQL17();
      long endTime = System.nanoTime();
      System.out.println((endTime - startTime)/1000000000.0);
    }
    else if (args[3].equals("allg")){
      long startTime = System.nanoTime();
      runQ1();
      runQ6();
      runQ15();
      runQ17();
      long endTime = System.nanoTime();
      System.out.println((endTime - startTime)/1000000000.0);
    }
  }

  private static void runQ1() throws ParseException {
    System.out.println(Query1.run(lineitem));
  }

  private static void runQ6() throws ParseException {
    System.out.println(Query6.run(lineitem));
  }

  private static void runQ15() throws ParseException {
    System.out.println(Query15.run(lineitem, supplier));
  }

  private static void runQ17() {
    System.out.println(Query17.run(lineitem, part));
  }

  private static void runSQL1() {
    // register views
    lineitem_sql.createOrReplaceTempView("lineitem");
    part_sql.createOrReplaceTempView("part");
    supplier_sql.createOrReplaceTempView("supplier");

    Dataset<Row> query1 = spark.sql("select\n" +
            "          l_returnflag,\n" +
            "          l_linestatus,\n" +
            "          sum(l_quantity) as sum_qty,\n" +
            "          sum(l_extendedprice) as sum_base_price,\n" +
            "          sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n" +
            "          sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n" +
            "          avg(l_quantity) as avg_qty,\n" +
            "          avg(l_extendedprice) as avg_price,\n" +
            "          avg(l_discount) as avg_disc,\n" +
            "          count(*) as count_order\n" +
            "        from\n" +
            "          lineitem\n" +
            "        where\n" +
            "          l_shipdate <= date '1998-12-01' - interval '90' day\n" +
            "        group by\n" +
            "          l_returnflag,\n" +
            "          l_linestatus\n" +
            "        order by\n" +
            "          l_returnflag,\n" +
            "          l_linestatus");

    //query1.collect();
    query1.show();
  }

  private static void runSQL6() {
    // register views
    lineitem_sql.createOrReplaceTempView("lineitem");
    part_sql.createOrReplaceTempView("part");
    supplier_sql.createOrReplaceTempView("supplier");

    Dataset<Row> query6 = spark.sql("select\n" +
            "          sum(l_extendedprice * l_discount) as revenue\n" +
            "        from\n" +
            "          lineitem\n" +
            "        where\n" +
            "          l_shipdate >= date '1994-01-01'\n" +
            "          and l_shipdate < date '1994-01-01' + interval '1' year\n" +
            "          and l_discount between .06 - 0.01 and .06 + 0.01\n" +
            "          and l_quantity < 24");

    query6.logicalPlan();
    query6.show();
  }

  private static void runSQL15() {
    // register views
    lineitem_sql.createOrReplaceTempView("lineitem");
    part_sql.createOrReplaceTempView("part");
    supplier_sql.createOrReplaceTempView("supplier");

    Dataset<Row> query15 = spark.sql("with revenue0 as\n" +
            "\t(select\n" +
            "\t\tl_suppkey as supplier_no,\n" +
            "\t\tsum(l_extendedprice * (1 - l_discount)) as total_revenue\n" +
            "\tfrom\n" +
            "\t\tlineitem\n" +
            "\twhere\n" +
            "\t\tl_shipdate >= date '1996-01-01'\n" +
            "\t\tand l_shipdate < date '1996-01-01' + interval '3' month\n" +
            "\tgroup by\n" +
            "\t\tl_suppkey)\n" +
            "\n" +
            "\n" +
            "select\n" +
            "\ts_suppkey,\n" +
            "\ts_name,\n" +
            "\ts_address,\n" +
            "\ts_phone,\n" +
            "\ttotal_revenue\n" +
            "from\n" +
            "\tsupplier,\n" +
            "\trevenue0\n" +
            "where\n" +
            "\ts_suppkey = supplier_no\n" +
            "\tand total_revenue = (\n" +
            "\t\tselect\n" +
            "\t\t\tmax(total_revenue)\n" +
            "\t\tfrom\n" +
            "\t\t\trevenue0\n" +
            "\t)\n" +
            "order by\n" +
            "\ts_suppkey");

    query15.show();
  }

  private static void runSQL17() {
    // register views
    lineitem_sql.createOrReplaceTempView("lineitem");
    part_sql.createOrReplaceTempView("part");
    supplier_sql.createOrReplaceTempView("supplier");

    Dataset<Row> query17 = spark.sql("select\n" +
            "\tsum(l_extendedprice) / 7.0 as avg_yearly\n" +
            "from\n" +
            "\tlineitem,\n" +
            "\tpart\n" +
            "where\n" +
            "\tp_partkey = l_partkey\n" +
            "\tand p_brand = 'Brand#23'\n" +
            "\tand p_container = 'MED BOX'\n" +
            "\tand l_quantity < (\n" +
            "\t\tselect\n" +
            "\t\t\t0.2 * avg(l_quantity)\n" +
            "\t\tfrom\n" +
            "\t\t\tlineitem\n" +
            "\t\twhere\n" +
            "\t\t\tl_partkey = p_partkey\n" +
            "\t)");

    query17.show();
  }
}