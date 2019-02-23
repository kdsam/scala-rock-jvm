package stellar

import scala.collection.mutable.ListBuffer
import scala.util.Try

/**
  * TODO: Documentation
  */
object HiveQueryOptimizer {

  def optimize(sql: String): Try[String] = {
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
            qstring += parseWhereExpr(l2child)
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
      var selFieldCtr = 0;
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

  def parseWhereExpr(ast: ASTNode): String = {
    var whereStr = BLANK + SQL_WHERE
    val child1 = ast.getChild(0).asInstanceOf[ASTNode]
    if (child1.getToken.getType == TOK_SUBQUERY_EXPR)
      whereStr += parseToSql(child1)
    else {
      val rootExpr = parseWhere(ast)
      whereStr += getWhereStr(rootExpr)
    }
    whereStr
  }

  private def parseWhere(ast: ASTNode): Expr = {
    ast.getToken.getType match {
      case TOK_WHERE =>
        val root = new LogicalExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          root.children += parseWhere(child)
        }
        root
      case KW_OR =>
        val orExpr = OrLogicalExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          orExpr.children += parseWhere(child)
        }
        orExpr
      case KW_AND =>
        val andExpr = AndLogicalExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          andExpr.children += parseWhere(child)
        }
        andExpr
      case EQUAL | EQUAL_NS | GREATERTHAN | GREATERTHANOREQUALTO | LESSTHAN |
           LESSTHANOREQUALTO  =>
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
        BinaryExpr(column, ast.getToken.getText, ConstantExpr(value))
      case _ => new LogicalExpr(ListBuffer())
    }
  }

  private def getWhereStr(rootExpr: Expr): String = {
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
              if (counter == 1) andStr += BLANK + OPEN_PAREN  + (getWhereStr(child)).substring(1) +
                CLOSE_PAREN
              else andStr += BLANK + SQL_AND + BLANK + OPEN_PAREN  + (getWhereStr(child)).substring(1) +
                CLOSE_PAREN
            case _ =>
              if (counter == 1)  andStr += getWhereStr(child)
              else andStr += BLANK + SQL_AND + getWhereStr(child)
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
              if (counter == 1) orStr += BLANK + OPEN_PAREN + (getWhereStr(child)).substring(1) +
                CLOSE_PAREN
              else orStr += BLANK + SQL_OR + BLANK + OPEN_PAREN + (getWhereStr(child)).substring(1) +
                CLOSE_PAREN
            case _ =>
              if (counter == 1)  orStr += getWhereStr(child)
              else orStr += BLANK + SQL_OR + getWhereStr(child)
          }
        }
        orStr
      case LogicalExpr(children) =>
        var lStr = EMPTY
        for {
          child <- children.toList
          if child != null
        } {
          lStr += getWhereStr(child)
        }
        lStr
      case BinaryExpr(column, operator, constantExpression) =>
        val bStr = (if (column.alias.isEmpty) BLANK
                    else BLANK + column.alias.get + SL_DOT) +
          s"${column.name} $operator ${constantExpression.value}"
        applyOptimization(bStr, column, operator, constantExpression)
//      case _ => ""  // forced to throw error for unhandled cases for now
    }
  }

  private def applyOptimization(bStr: String,
                                column: Column,
                                operator: String,
                                constantExpression: ConstantExpr): String = {
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

  case class Table(name: String,
                   db: Option[String] = None,
                   alias: Option[String] = None)
      extends Expr

  case class Column(name: String, alias: Option[String] = None)

  case class BinaryExpr(column: Column,
                        operator: String,
                        constantExpression: ConstantExpr)
      extends Expr

  sealed trait SelectExpr extends Expr

  case class SelectAll(db: Option[String] = None) extends SelectExpr
  case class SelectField(column: Column) extends SelectExpr

  // TODO: Do typing on this, right now this is all String
  case class ConstantExpr(value: String) extends Expr

  case class HiveQuery(original: String, suggested: String)
}
