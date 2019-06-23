package helper

import java.sql.{DriverManager, ResultSet, SQLException}
import java.util.Properties
import org.apache.calcite.jdbc.CalciteConnection
import org.apache.calcite.rel.RelNode
import org.apache.calcite.sql.parser.SqlParser
import org.apache.calcite.tools.{Frameworks, RelBuilder, RelRunners}

object CalciteUtils {
  def getRelBuilder(pathToJson: String): RelBuilder = {
    Class.forName("org.apache.calcite.jdbc.Driver")
    val info = new Properties
    info.put("model", pathToJson)
    info.setProperty("lex", "JAVA")

    val calciteConnection =
      try {
        DriverManager.getConnection("jdbc:calcite:", info)
          .unwrap(classOf[CalciteConnection])
      } catch {
        case e: SQLException => {
          println("Problem when setting up the jdbc connection to you data base. Check the json file for the schema.")
          throw  e
        }
      }

    val frameworkConfig =
      Frameworks.newConfigBuilder
      .parserConfig(SqlParser.Config.DEFAULT)
      .defaultSchema(calciteConnection.getRootSchema.getSubSchema("foodmart"))
      .build()

    RelBuilder.create(frameworkConfig)
  }

  def printRows(node: RelNode): Unit = {

    val preparedStatement = RelRunners.run(node)
    val numberOfColumns = preparedStatement.getMetaData.getColumnCount
    // sql exception
    val resultSet: ResultSet = preparedStatement.executeQuery

    while (resultSet.next()) {
      for (i <- 1 to numberOfColumns) {
        if (i != 1)
          print(", ")
        print(preparedStatement.getMetaData.getColumnName(i) + ": " + resultSet.getString(i))
      }
      println
    }
  }
}
