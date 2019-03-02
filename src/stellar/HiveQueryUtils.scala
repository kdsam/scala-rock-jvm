package stellar

object HiveQueryUtils {

  val EQUAL_OPERATOR = "="
  val TRUE_EXPR = "true"
  val FALSE_EXPR = "false"
  val SL_DOT = "."
  val SL_COMMA = ","
  val EMPTY = ""
  val BLANK = " "
  val OPEN_PAREN = "("
  val CLOSE_PAREN =")"
  val BT_REPLACE_START = "bt__"
  val BT_REPLACE_END = "__bt"
  val BT_REPLACE_SPACE = "_bts_"
  val HIVE_ACC_QUOTE = "`"
  val SL_PLUS = "+"

  /*** SQL KEYWORDS ***/
  val SQL_SELECT = "SELECT"
  val SQL_IN = "IN"
  val SQL_EXISTS = "EXISTS"
  val SQL_FROM = "FROM"
  val SQL_ALL = "*"
  val SQL_WHERE = "WHERE"
  val SQL_AND = "AND"
  val SQL_OR = "OR"
  val SQL_GROUP_BY = "GROUP BY"
  val SQL_HAVING = "HAVING"
  val SQL_ORDER_BY = "ORDER BY"
  val SQL_ASC = "ASC"
  val SQL_DESC = "DESC"
  val SQL_INNER_JOIN = "INNER JOIN"
  val SQL_LEFT_JOIN = "LEFT JOIN"
  val SQL_RIGHT_JOIN = "RIGHT JOIN"
  val SQL_FULL_OUTER_JOIN = "FULL OUTER JOIN"
  val SQL_ON = "ON"
  val SQL_AS = "AS"
  val SQL_UNION_ALL = "UNION ALL"
  val SQL_DISTINCT = "DISTINCT"
  val SQL_NOT = "NOT"
  val SQL_IS_NULL = "IS NULL"
  val SQL_IS_NOT_NULL = "IS NOT NULL"
  val SQL_LIMIT = "LIMIT"
  val SQL_BETWEEN = "BETWEEN"
  val SQL_CASE = "CASE"
  val SQL_WHEN = "WHEN"
  val SQL_THEN = "THEN"
  val SQL_ELSE = "ELSE"
  val SQL_END = "END"
  val SQL_CAST = "CAST"
  val SQL_DATE = "DATE"
  val SQL_TIMESTAMP = "TIMESTAMP"

  def copyAST(orig: ASTNode): ASTNode = {
    val ast = new ASTNode(orig)
    ast
  }
}
