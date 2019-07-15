import helper.CalciteUtils
import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.JoinRelType
import org.apache.calcite.tools.RelBuilder

object Main {
  def main(args: Array[String]): Unit = {
    // you call this application with the path to the schema json
    val pathToJsonCsv = args(0)
    val pathToJsonPostgres = args(1)
    val builderCsv = CalciteUtils.getRelBuilder(pathToJsonCsv)
    val builder = CalciteUtils.getRelBuilder(pathToJsonPostgres)

    val nodeSampleCsv = getNodeFromSampleCsvSchema(builderCsv)
    println("Sample CSV Relation")
    println(RelOptUtil.toString(nodeSampleCsv))
    println()

    val nodeTaskOne = getNodeOne(builder)
    println("Relation Task 1")
    println(RelOptUtil.toString(nodeTaskOne))
    println()

    val nodeTaskTwo = getNodeTwo(builder)
    println("Relation Task 2")
    println(RelOptUtil.toString(nodeTaskTwo))
    println()

    val nodeTaskThree = getNodeThree(builder)
    println("Relation Task 3")
    println(RelOptUtil.toString(nodeTaskThree))

    println("Data Task 1")
    CalciteUtils.printRows(nodeTaskOne)
    println

    println("Data Task 2")
    CalciteUtils.printRows(nodeTaskTwo)
    println

    println("Data Task 3")
    CalciteUtils.printRows(nodeTaskThree)
    println
  }

  def getNodeOne(builder: RelBuilder): RelNode = {
    /* equivalent query
        SELECT ti.the_year
             , SUM(sa.unit_sales)

        FROM sales_fact_1998 AS sa

        INNER JOIN customer AS cu
        ON cu.customer_id = sa.customer_id

        INNER JOIN time_by_day AS ti
        ON ti.time_id = sa.time_id

        WHERE cu.city = 'Albany'

        GROUP BY ti.the_year;
    */

    builder
      .scan("sales_fact_1998")
      .scan("customer")
      .filter(
        builder.equals(
          builder.field("city"),
          builder.literal("Albany")))
      .join(JoinRelType.INNER, "customer_id")
      .scan("time_by_day")
      .join(JoinRelType.INNER, "time_id")
      .aggregate(
        builder.groupKey("the_year"),
        builder.sum(false, "sum_unit_sales", builder.field("unit_sales")))
      .build
  }

  def getNodeTwo(builder: RelBuilder): RelNode = {
  /* equivalent query
      SELECT ti.the_year
           , ti.day_of_month
           , SUM(sa.unit_sales)

      FROM sales_fact_1998 AS sa

      INNER JOIN customer AS cu
      ON cu.customer_id = sa.customer_id

      INNER JOIN time_by_day AS ti
      ON ti.time_id = sa.time_id

      WHERE cu.city = 'Albany'

      GROUP BY ti.the_year
             , ti.day_of_month;
  */
    builder
      .scan("sales_fact_1998")
      .scan("customer")
      .join(JoinRelType.INNER, "customer_id")
      .filter(builder.equals(builder.field("city"), builder.literal("Albany")))
      .scan("time_by_day")
      .join(JoinRelType.INNER, "time_id")
      .aggregate(
        builder.groupKey("the_year", "day_of_month"),
        builder.sum(false, "sum_unit_sales", builder.field("unit_sales")))
      .build
  }

  def getNodeThree(builder: RelBuilder): RelNode = {
    /* equivalent query
        SELECT DISTINCT
               cu.fullname
             , SUM(sa.unit_sales) OVER(
                 PARTITION BY cu.customer_id
               )

        FROM sales_fact_1998 AS sa

        INNER JOIN customer AS cu
        ON cu.customer_id = sa.customer_id

        WHERE cu.city = 'Albany'

        ORDER BY 2 DESC

        LIMIT 5;
    */
    builder
      .scan("sales_fact_1998")
      .scan("customer")
      .join(JoinRelType.INNER, "customer_id")
      .filter(
        builder.equals(
          builder.field("city"),
          builder.literal("Albany")))
      .aggregate(
        builder.groupKey("fullname"),
        builder.sum(false,
          "sum_unit_sales",
          builder.field( "unit_sales")))

      .project(builder.field("fullname"))
      .distinct()
      .sortLimit(-1, 5, builder.desc(builder.field("fullname")))
      .build()
  }

  def getNodeFromSampleCsvSchema(builder: RelBuilder): RelNode = {
    builder
      .scan("food").as("f")
      .scan("food_de").as("f_de")
      .join(JoinRelType.INNER, "food_id")
      .project(
        builder.field("f", "food_name"),
        builder.field("f_de", "food_name"),
        builder.field("f", "food_price")
      )
      .build()
  }
}
