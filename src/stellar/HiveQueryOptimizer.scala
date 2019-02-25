package stellar

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * TODO: Documentation
  */
object HiveQueryOptimizer {

  def optimize(sql: String): OptimizeResult = {
    val res = tryOptimize(sql)
    res match {
      case Success(value) =>
        OptimizeResult(sql, true, value, None)
      case Failure(e) =>
        OptimizeResult(sql, false, sql, Some(e))
    }
  }

  def tryOptimize(sql: String): Try[String] = {
    for {
      node <- Try(ParseUtils.parse(sql))
    } yield {
      parseToSql(node)
    }
  }

  def parseToSql(ast: ASTNode): String = {

    ast.getToken.getType match {
      case TOK_QUERY =>
        var qstring = SQL_SELECT
        // get SELECT
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_INSERT
        } {
          qstring += parseSelectExpr(child)
        }

        // get FROM
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_FROM
        } {
          qstring += parseFrom(child)
        }

        // get WHERE
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_INSERT
        } {
          for {
            l2child <- child.getChildren.toList
            if l2child != null && l2child.getToken.getType == TOK_WHERE
          } {
            if (ast.getToken.getType == TOK_SUBQUERY_EXPR) parseToSql(ast)
            qstring += parseWhere(l2child)
          }
        }

        // get GROUP BY
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_INSERT
        } {
          for {
            l2child <- child.getChildren.toList
            if l2child != null && l2child.getToken.getType == TOK_GROUPBY
          } {
            qstring += parseGroupBy(l2child)
          }
        }

        // get HAVING
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_INSERT
        } {
          for {
            l2child <- child.getChildren.toList
            if l2child != null && l2child.getToken.getType == TOK_HAVING
          } {
            qstring += parseHaving(l2child)
          }
        }

        // get ORDER BY
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_INSERT
        } {
          for {
            l2child <- child.getChildren.toList
            if l2child != null && l2child.getToken.getType == TOK_ORDERBY
          } {
            qstring += parseOrderBy(l2child)
          }
        }

        qstring
      case TOK_SUBQUERY_EXPR =>
        var appendStr = EMPTY
        // parse Subquery Operation
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_SUBQUERY_OP
        } {
          for {
            child <- ast.getChildren.toList
            if child != null && child.getToken.getType == TOK_TABLE_OR_COL
          } {
            child.getChildren.toList match {
              case c1 :: Nil => appendStr += BLANK + c1.getToken.getText
//              case _ => // forced to throw error for unhandled cases
            }
          }
          appendStr += BLANK + SQL_IN
        }

        // parse Subquery
        appendStr += OPEN_PAREN
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_QUERY
        } {
          appendStr += parseToSql(child)
        }
        appendStr += CLOSE_PAREN
        appendStr
//      case _ => "" // forced to throw error for unhandled cases
    }
  }

  def parseFrom(ast: ASTNode): String = {
    var appendStr = EMPTY
    var db = EMPTY
    var table = EMPTY
    var alias = EMPTY
    for {
      child <- ast.getChildren.toList
      if child != null && child.getToken.getType == TOK_TABREF
    } {
      for {
        l2child <- child.getChildren.toList
        if l2child != null
      } {
        l2child.getToken.getType match {
          case TOK_TABNAME =>
            l2child.getChildren.toList match {
              case c1 :: c2 :: Nil =>
                db = c1.getText
                table = c2.getText
              case _ => table = l2child.getChild(0).getText
            }
          case _ => alias = l2child.getText
        }
      }
    }
    appendStr += BLANK + SQL_FROM + BLANK + (if (db.isEmpty) EMPTY
                                             else
                                               db + SL_DOT) + table + (if (alias.isEmpty)
                                                                         EMPTY
                                                                       else
                                                                         BLANK + alias)
    appendStr
  }

  def parseSelectExpr(ast: ASTNode): String = {
    var appendStr = EMPTY
    for {
      child <- ast.getChildren.toList
      if child != null && child.getToken.getType == TOK_SELECT
    } {
      var selFieldCtr = 0
      for {
        l2child <- child.getChildren.toList
        if l2child != null && l2child.getToken.getType == TOK_SELEXPR
      } {
        for {
          l3child <- l2child.getChildren.toList
          if l3child != null
        } {
          l3child.getToken.getType match {
            case TOK_ALLCOLREF =>
              selFieldCtr += 1
              appendStr += BLANK
              if (selFieldCtr > 1) appendStr += SL_COMMA
              if (l3child.getChild(0) != null) {
                appendStr += l3child
                  .getChild(0)
                  .getChild(0)
                  .asInstanceOf[ASTNode]
                  .getToken
                  .getText + SL_DOT
              }
              appendStr += SQL_ALL
            case TOK_TABLE_OR_COL =>
              selFieldCtr += 1
              l3child.getChildren.toList match {
                case c1 :: Nil =>
                  if (selFieldCtr > 1) appendStr += SL_COMMA
                  appendStr += BLANK + c1.getToken.getText
//              case _ => // forced to throw error for unhandled cases
              }
            case DOT =>
              selFieldCtr += 1
              if (selFieldCtr > 1) appendStr += SL_COMMA
              appendStr += BLANK
              appendStr += l3child
                .getChild(0)
                .getChild(0)
                .asInstanceOf[ASTNode]
                .getToken
                .getText + SL_DOT
              appendStr += l3child
                .getChild(1)
                .asInstanceOf[ASTNode]
                .getToken
                .getText
//            case _ => // forced to throw error for unhandled cases
          }
        }
      }
    }
    appendStr
  }

  def parseWhere(ast: ASTNode): String = {
    var whereStr = BLANK + SQL_WHERE
    val child1 = ast.getChild(0).asInstanceOf[ASTNode]
    if (child1.getToken.getType == TOK_SUBQUERY_EXPR)
      whereStr += parseToSql(child1)
    else {
      val rootExpr = parseToExpr(ast)
      whereStr += getStrFromExpr(rootExpr)
    }
    whereStr
  }

  private def parseToExpr(ast: ASTNode): Expr = {
    ast.getToken.getType match {
      case TOK_WHERE =>
        val root = new LogicalExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          root.children += parseToExpr(child)
        }
        root
      case TOK_HAVING =>
        val root = new HavingExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          root.children += parseToExpr(child)
        }
        root
      case KW_OR =>
        val orExpr = OrLogicalExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          orExpr.children += parseToExpr(child)
        }
        orExpr
      case KW_AND =>
        val andExpr = AndLogicalExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          andExpr.children += parseToExpr(child)
        }
        andExpr
      case EQUAL | EQUAL_NS | GREATERTHAN | GREATERTHANOREQUALTO | LESSTHAN |
           LESSTHANOREQUALTO  =>
        ast.getChild(0).asInstanceOf[ASTNode].getToken.getType match {
          case TOK_TABLE_OR_COL | DOT => getColumnBinaryExpression(ast)
          case TOK_FUNCTION | MINUS  => getFunctionBinaryExpression(ast)
//          case MINUS => getBinaryExpression(ast)
        }
//      case _ => // force to throw exception
    }
  }

//  private def getBinaryExpression(ast: ASTNode): BinaryExpr = {
//    ast.getToken.getType match {
//      case
//    }
//  }

  private def getFunctionBinaryExpression(ast: ASTNode): BinaryExpr = {
    ast.getChildren.toList match {
      case c1 :: c2 :: Nil =>
        val leftBinaryVal = getBinaryVal(c1)
        val rightBinaryVal = getBinaryVal(c2)
        BinaryExpr(leftBinaryVal, ast.getToken.getText, rightBinaryVal)
//      case _ => // force to throw exception
    }
  }

  private def getBinaryVal(ast: ASTNode): BinaryVal = {
    ast.getToken.getType match {
      case TOK_FUNCTION =>
        val functionName = ast.getChild(0).getText
        val params = ListBuffer[BinaryVal]()
        params += getBinaryVal(ast.getChild(1).asInstanceOf[ASTNode])
        SqlFunction(functionName, params: _*)
      case TOK_TABLE_OR_COL =>
        ast.getChildren.toList match {
          case c1 :: Nil => Column(c1.getText)
        }
      case Number => NumberConstantExpr(ast.getText)
      case MINUS =>
        val leftBinaryVal = getBinaryVal(ast.getChild(0).asInstanceOf[ASTNode])
        val rightBinaryVal = getBinaryVal(ast.getChild(1).asInstanceOf[ASTNode])
        BinaryExpr(leftBinaryVal, ast.getToken.getText, rightBinaryVal)
//      case _ => // force to throw exception
    }
  }

  private def getColumnBinaryExpression(ast: ASTNode): BinaryExpr = {
    var colName: String = EMPTY
    var tableAlias: String = EMPTY
    var value: String = EMPTY
    for {
      child <- ast.getChildren.toList
      if child != null
    } {
      child.getToken.getType match {
        case TOK_TABLE_OR_COL =>
          colName = child.getChild(0).getText
        case DOT =>
          tableAlias = child.getChild(0).getChild(0).getText
          colName = child.getChild(1).getText
        case KW_TRUE =>
          value = TRUE_EXPR
        case KW_FALSE =>
          value = FALSE_EXPR
        case StringLiteral =>
          value = child.getText
        case Number =>
          value = child.getText
      }
    }
    val column =
      if (tableAlias.isEmpty) Column(colName)
      else Column(colName, Some(tableAlias))
    BinaryExpr(column, ast.getToken.getText, StringConstantExpr(value))
  }

  private def getStrFromExpr(rootExpr: Expr): String = {
    rootExpr match {
      case AndLogicalExpr(children) =>
        var andStr = EMPTY
        var counter = 0
        for {
          child <- children.toList
          if child != null
        } {
          counter += 1
          child match {
            case LogicalExpr(_) =>
              if (counter == 1) andStr += BLANK + OPEN_PAREN  + getStrFromExpr(child).substring(1) +
                CLOSE_PAREN
              else andStr += BLANK + SQL_AND + BLANK + OPEN_PAREN  + getStrFromExpr(child).substring(1) +
                CLOSE_PAREN
            case _ =>
              if (counter == 1)  andStr += getStrFromExpr(child)
              else andStr += BLANK + SQL_AND + getStrFromExpr(child)
          }
        }
        andStr
      case OrLogicalExpr(children) =>
        var orStr = EMPTY
        var counter = 0
        for {
          child <- children.toList
          if child != null
        } {
          counter += 1
          child match {
            case LogicalExpr(_) =>
              if (counter == 1) orStr += BLANK + OPEN_PAREN + getStrFromExpr(child).substring(1) +
                CLOSE_PAREN
              else orStr += BLANK + SQL_OR + BLANK + OPEN_PAREN + getStrFromExpr(child).substring(1) +
                CLOSE_PAREN
            case _ =>
              if (counter == 1)  orStr += getStrFromExpr(child)
              else orStr += BLANK + SQL_OR + getStrFromExpr(child)
          }
        }
        orStr
      case LogicalExpr(children) =>
        var lStr = EMPTY
        for {
          child <- children.toList
          if child != null
        } {
          lStr += getStrFromExpr(child)
        }
        lStr
      case HavingExpr(children) =>
        var havingStr = EMPTY
        for {
          child <- children.toList
          if child != null
        } {
          havingStr += getStrFromExpr(child)
        }
        havingStr
      case BinaryExpr(leftVal, operator, rightVal) =>
        val leftStr = getStrFromExpr(leftVal)
        val rightStr = getStrFromExpr(rightVal)
        val bStr = leftStr + BLANK +  operator + BLANK + rightStr.trim
        // TODO make this one dynamic
        if (leftVal.isInstanceOf[Column])
          applyOptimization(bStr, leftVal.asInstanceOf[Column], operator, rightVal.asInstanceOf[StringConstantExpr])
        else bStr
      case Column(name, alias) =>
        val colStr = (if (alias.isEmpty) BLANK
        else BLANK + alias.get + SL_DOT) + name
        colStr
      case SqlFunction(name, params @ _*) =>
        var funcStr = BLANK + name + OPEN_PAREN
        var paramCtr = 0;
        for{
          binaryVal <- params
        } {
          paramCtr += 1
          if (paramCtr > 1) funcStr += SL_COMMA + BLANK
          if (paramCtr == 1) funcStr += getStrFromExpr(binaryVal).substring(1)
          else funcStr += getStrFromExpr(binaryVal)
        }
        funcStr += CLOSE_PAREN
        funcStr
      case StringConstantExpr(value) => value
      case NumberConstantExpr(value) => value.toString
//      case _ => ""  // forced to throw error for unhandled cases for now
    }
  }

  private def applyOptimization(bStr: String,
                                column: Column,
                                operator: String,
                                constantExpression: StringConstantExpr): String = {
    var optimized = bStr
    // TODO: make optimization fields dynamic instead of hardcoding
    if (column.name.equalsIgnoreCase("member_id")) {
      val constantVal = constantExpression.value
      val partitionVal = constantVal.substring(constantVal.length - 3)
      optimized += BLANK + SQL_AND + (if (column.alias.isEmpty) BLANK
                                      else BLANK + column.alias.get + SL_DOT) +
        s"last_digits $operator '$partitionVal"
    }
    optimized
  }

  def parseGroupBy(ast: ASTNode): String = {
    var groupByStr = EMPTY
    ast.getToken.getType match {
      case TOK_GROUPBY =>
        groupByStr += BLANK + SQL_GROUP_BY
        var fieldCtr = 0
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          child.getToken.getType match {
            case DOT => groupByStr += parseGroupBy(child)
            case TOK_TABLE_OR_COL => groupByStr += parseGroupBy(child)
            case TOK_TABSORTCOLNAMEASC =>
              fieldCtr += 1
              if (fieldCtr > 1) groupByStr += SL_COMMA
              groupByStr += parseGroupBy(
                child.getChild(0).asInstanceOf[ASTNode]) + BLANK + SQL_ASC
            case TOK_TABSORTCOLNAMEDESC =>
              fieldCtr += 1
              if (fieldCtr > 1) groupByStr += ","
              groupByStr += parseGroupBy(
                child.getChild(0).asInstanceOf[ASTNode]) + BLANK + SQL_DESC
//            case _ => // force to throw exception for now
          }
        }
      case TOK_NULLS_FIRST | TOK_NULLS_LAST =>
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          groupByStr += parseGroupBy(child)
        }
      case TOK_TABLE_OR_COL =>
        ast.getChildren.toList match {
          case c1 :: Nil => groupByStr += BLANK + c1.getToken.getText
//          case _ => // forced to throw error for unhandled cases
        }
      case DOT =>
        ast.getChildren.toList match {
          case c1 :: c2 :: Nil =>
            groupByStr += BLANK + c1
              .getChild(0)
              .asInstanceOf[ASTNode]
              .getToken
              .getText
            groupByStr += SL_DOT + c2.getToken.getText
//          case _ => // forced to throw error for unhandled cases
        }

//      case _ => // force to throw exception for now
    }
    groupByStr
  }

  def parseHaving(ast: ASTNode): String = {
    var havingStr = BLANK + SQL_HAVING
    val rootExpr = parseToExpr(ast)
    havingStr += getStrFromExpr(rootExpr)
    havingStr
  }

  private def parseHavingToExpr(node: ASTNode): Expr = ???

  private def getHavingStr(rootExpr: Expr): String = ???
  //  def parseHaving2(ast: ASTNode): String = {
//    var havingStr = EMPTY
//    ast.getToken.getType match {
//      case TOK_HAVING =>
//        havingStr += BLANK + SQL_ORDER_BY
//        var fieldCtr = 0
//        for {
//          child <- ast.getChildren.toList
//          if child != null
//        } {
//          child.getToken.getType match {
//            case TOK_TABSORTCOLNAMEASC =>
//              fieldCtr += 1
//              if (fieldCtr > 1) orderByStr += SL_COMMA
//              orderByStr += parseOrderBy(
//                child.getChild(0).asInstanceOf[ASTNode]) + BLANK + SQL_ASC
//            case TOK_TABSORTCOLNAMEDESC =>
//              fieldCtr += 1
//              if (fieldCtr > 1) orderByStr += ","
//              orderByStr += parseOrderBy(
//                child.getChild(0).asInstanceOf[ASTNode]) + BLANK + SQL_DESC
//            //            case _ => // force to throw exception for now
//          }
//        }
//      case TOK_NULLS_FIRST | TOK_NULLS_LAST =>
//        for {
//          child <- ast.getChildren.toList
//          if child != null
//        } {
//          orderByStr += parseOrderBy(child)
//        }
//      case TOK_TABLE_OR_COL =>
//        ast.getChildren.toList match {
//          case c1 :: Nil => orderByStr += BLANK + c1.getToken.getText
//          //          case _ => // forced to throw error for unhandled cases
//        }
//      case DOT =>
//        ast.getChildren.toList match {
//          case c1 :: c2 :: Nil =>
//            orderByStr += BLANK + c1
//              .getChild(0)
//              .asInstanceOf[ASTNode]
//              .getToken
//              .getText
//            orderByStr += SL_DOT + c2.getToken.getText
//          //          case _ => // forced to throw error for unhandled cases
//        }
//
//      //      case _ => // force to throw exception for now
//    }
//    orderByStr
//  }

  def parseOrderBy(ast: ASTNode): String = {
    var orderByStr = EMPTY
    ast.getToken.getType match {
      case TOK_ORDERBY =>
        orderByStr += BLANK + SQL_ORDER_BY
        var fieldCtr = 0
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          child.getToken.getType match {
            case TOK_TABSORTCOLNAMEASC =>
              fieldCtr += 1
              if (fieldCtr > 1) orderByStr += SL_COMMA
              orderByStr += parseOrderBy(
                child.getChild(0).asInstanceOf[ASTNode]) + BLANK + SQL_ASC
            case TOK_TABSORTCOLNAMEDESC =>
              fieldCtr += 1
              if (fieldCtr > 1) orderByStr += ","
              orderByStr += parseOrderBy(
                child.getChild(0).asInstanceOf[ASTNode]) + BLANK + SQL_DESC
            //            case _ => // force to throw exception for now
          }
        }
      case TOK_NULLS_FIRST | TOK_NULLS_LAST =>
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          orderByStr += parseOrderBy(child)
        }
      case TOK_TABLE_OR_COL =>
        ast.getChildren.toList match {
          case c1 :: Nil => orderByStr += BLANK + c1.getToken.getText
          //          case _ => // forced to throw error for unhandled cases
        }
      case DOT =>
        ast.getChildren.toList match {
          case c1 :: c2 :: Nil =>
            orderByStr += BLANK + c1
              .getChild(0)
              .asInstanceOf[ASTNode]
              .getToken
              .getText
            orderByStr += SL_DOT + c2.getToken.getText
          //          case _ => // forced to throw error for unhandled cases
        }

      //      case _ => // force to throw exception for now
    }
    orderByStr
  }

  implicit class ArrayListConverter(val arrayList: java.util.ArrayList[Node])
      extends AnyVal {
    def toList: List[ASTNode] = {
      if (arrayList != null) {
        import scala.collection.JavaConverters._
        arrayList.asScala.map(_.asInstanceOf[ASTNode]).toList
      } else {
        List.empty[ASTNode]
      }
    }
  }

  sealed trait Expr

  class LogicalExpr(val children: ListBuffer[Expr]) extends Expr
  object LogicalExpr {
    def unapply(arg: LogicalExpr): Option[ListBuffer[Expr]] = Some(arg.children)
  }
  case class AndLogicalExpr(override val children: ListBuffer[Expr])
      extends LogicalExpr(children)
  case class OrLogicalExpr(override val children: ListBuffer[Expr])
      extends LogicalExpr(children)

  class HavingExpr(val children: ListBuffer[Expr]) extends Expr

  object HavingExpr {
    def unapply(arg: HavingExpr): Option[ListBuffer[Expr]] = Some(arg.children)
  }

  case class Table(name: String,
                   db: Option[String] = None,
                   alias: Option[String] = None)
      extends Expr

  sealed trait BinaryVal extends Expr

  case class Column(name: String, alias: Option[String] = None) extends BinaryVal

  case class SqlFunction(funcName: String, params: BinaryVal*) extends BinaryVal

  case class BinaryExpr(leftVal: BinaryVal,
                        operator: String,
                        rightVal: BinaryVal)
      extends BinaryVal

  sealed trait SelectExpr extends Expr

  case class SelectAll(db: Option[String] = None) extends SelectExpr
  case class SelectField(column: Column) extends SelectExpr

  // TODO: Do typing on this, right now this is all String
  case class StringConstantExpr(value: String) extends BinaryVal

  case class NumberConstantExpr(value: String) extends BinaryVal

  case class HiveQuery(original: String, suggested: String)

  case class OptimizeResult(original: String, optimized: Boolean, result: String, errorOpt: Option[Throwable])
}
