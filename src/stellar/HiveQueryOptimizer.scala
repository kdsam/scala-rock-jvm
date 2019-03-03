package stellar

import scala.collection.mutable.ListBuffer
import scala.util.{Failure, Success, Try}

/**
  * <p>
  *   This is a Utility class for Parsing SQL statement to
  *   AST, applying optimizations from input arguments, and then
  *   transforming it back to the optimized SQL String.
  *   <br>
  *   <br>
  *   <i>Note: The purpose of this Optimizer is for SELECT queries only. This should not be used for DDL
  *   and DML Statements.</i>
  *   <br>
  *   <br>
  *   The following are supported and tested:
  *   <ol>
  *     <li>SIMPLE SELECT QUERIES</li>
  *     <li>VARIOUS SELECT CONSTRUCTS</li>
  *     <li>SELECT FIELDS </li>
  *     <li>WHERE CLAUSES </li>
  *     <li>GREATER THAN LESS THAN OPERATORS</li>
  *     <li>OR, AND and NOT LOGICAL OPERATORS W/ PARENTHESIS/GROUPING</li>
  *     <li>ORDER BY</li>
  *     <li>IS NULL</li>
  *     <li>SELECT With LIMIT</li>
  *     <li>MIN() and MAX()</li>
  *     <li>COUNT, AVG, SUM</li>
  *     <li>SQL LIKE</li>
  *     <li>SQL WILDCARDS</li>
  *     <li>SQL IN OPERATOR</li>
  *     <li>SQL BETWEEN</li>
  *     <li>SQL ALIASES</li>
  *     <li>JOIN</li>
  *     <li>UNION</li>
  *     <li>GROUP BY</li>
  *     <li>HAVING</li>
  *     <li>SQL EXISTS</li>
  *     <li>SQL CASE in SELECT And ORDER BY CLAUSES</li>
  *     <li>SUBQUERIES</li>
  *   </ol>
  * </p>
  * <p>
  *   <strong>Known Limitations:</strong>
  *   <br>
  *   <br>
  *   The following SQL are the known SQL KeyWords(from other DB providers) which aren't supported yet:
  *   <ol>
  *     <li>ANY in WHERE predicates</li>
  *     <li>ALL in WHERE predicates</li>
  *   </ol>
  * </p>
  */
object HiveQueryOptimizer {

  def optimize(sql: String, optimizeDefs: OptimizeDefinition*): OptimizeResult = {
    val res = tryOptimize(sql, optimizeDefs: _*)
    res match {
      case Success(value) =>
        OptimizeResult(sql, optimized = true, value, None)
      case Failure(e) =>
        OptimizeResult(sql, optimized = false, sql, Some(e))
    }
  }

  def tryOptimize(sql: String, optimizeDefs: OptimizeDefinition*): Try[String] = {
    for {
      node <- Try(parseToAst(sql))
    } yield {
      val optimized = optimizeAST(node, optimizeDefs: _*)
      parseToSql(optimized)
    }
  }

  def optimizeAST(ast: ASTNode, optimizeDefs: OptimizeDefinition*): ASTNode = {
    val opt = copyAST(ast)
    ast.getToken.getType match {
      case KW_AND =>
        for {
          child <- ast.getChildren.toList
        } {
          opt.addChild(optimizeAST(child, optimizeDefs: _*))
          val binExpr = parseToExpr(child)
          for {
            optDef <- optimizeDefs.toList
          } {
            // Do Optimization Here
            if (optDef.ifPresent(binExpr)) {
              val addtlOpt = optDef.applyOptimization(binExpr)
              val addtAst = createAstFor(addtlOpt, child)
              opt.addChild(addtAst)
            }
          }
        }
      case _ =>
        for {
          child <- ast.getChildren.toList
        } {
          opt.addChild(optimizeAST(child, optimizeDefs: _*))
        }
    }
    opt
  }

  def parseToAst(sql: String): ASTNode = {
    val sanitized = sanitizeSQLInput(sql)
    ParseUtils.parse(sanitized)
  }

  def parseToSql(ast: ASTNode): String = {
    val sql = parseAstToSql(ast)
    sanitizeSQLOutput(sql)
  }

  private def parseAstToSql(ast: ASTNode): String = {
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
            if (ast.getToken.getType == TOK_SUBQUERY_EXPR) parseAstToSql(ast)
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

        // get LIMIT
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_INSERT
        } {
          for {
            l2child <- child.getChildren.toList
            if l2child != null && l2child.getToken.getType == TOK_LIMIT
          } {
            qstring += parseLimit(l2child)
          }
        }

        qstring
      case TOK_SUBQUERY =>
        var subQueryStr = BLANK + OPEN_PAREN
        ast.getChildren.toList match {
          case c1 :: c2 :: Nil =>
            subQueryStr += parseAstToSql(c1) + CLOSE_PAREN + BLANK + c2.getText
          case c1 :: Nil =>
            subQueryStr += parseAstToSql(c1) + CLOSE_PAREN
          case _ => throw SLUnsupportedSQLConstruct(s"Supported children size: 1 or 2," +
            s" but found ${ast.getChildren.size}")
        }
        subQueryStr
      case TOK_SUBQUERY_EXPR =>
        var appendStr = EMPTY
        var prepend = SQL_IN

        // parse Column Information
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == DOT
        } {
          val binaryVal = getBinaryVal(child)
          appendStr += getStrFromExpr(binaryVal)
        }

        // parse Subquery Operation
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_SUBQUERY_OP
        } {
          if (child.getChild(0).asInstanceOf[ASTNode].getToken.getType == KW_EXISTS) prepend = SQL_EXISTS
          for {
            child <- ast.getChildren.toList
            if child != null && child.getToken.getType == TOK_TABLE_OR_COL
          } {
            child.getChildren.toList match {
              case c1 :: Nil => appendStr += BLANK + c1.getToken.getText
              case _ =>
                throw SLUnsupportedSQLConstruct(
                  s"Supported children size: 1, but found ${child.getChildren.size}")
            }
          }
          appendStr += BLANK + prepend
        }

        // parse Subquery
        appendStr += BLANK + OPEN_PAREN
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType == TOK_QUERY
        } {
          appendStr += parseAstToSql(child)
        }
        appendStr += CLOSE_PAREN
        appendStr
      case TOK_UNIONALL =>
        var unionStr = EMPTY
        var ctr = 0
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          ctr += 1
          if (ctr > 1) unionStr += BLANK + SQL_UNION_ALL + BLANK
          unionStr += parseAstToSql(child)
        }
        unionStr
      case _ => throw SLUnsupportedSQLConstruct(s"Unsupported Token Type: ${ast.getToken.getType}") // forced to throw error for unhandled cases
    }
  }

  private def parseFrom(ast: ASTNode): String = {
    var appendStr = BLANK + SQL_FROM
    for {
      child <- ast.getChildren.toList
    } {
      child.getToken.getType match {
        case TOK_JOIN | TOK_LEFTOUTERJOIN | TOK_RIGHTOUTERJOIN | TOK_FULLOUTERJOIN =>
          appendStr += parseJoin(child)
        case TOK_TABREF => appendStr += parseTableRef(child)
        case TOK_SUBQUERY => appendStr += parseAstToSql(child)
      }
    }
    appendStr
  }

  private def parseJoin(ast: ASTNode): String = {
    var joinStr = EMPTY
    val joinTypeStr = ast.getToken.getType match {
      case TOK_JOIN           => SQL_INNER_JOIN
      case TOK_LEFTOUTERJOIN  => SQL_LEFT_JOIN
      case TOK_RIGHTOUTERJOIN => SQL_RIGHT_JOIN
      case TOK_FULLOUTERJOIN => SQL_FULL_OUTER_JOIN
    }
    // get Join tables
    var tabrefCtr = 0
    for {
      child <- ast.getChildren.toList
      if child != null && (child.getToken.getType == TOK_TABREF || child.getToken.getType == TOK_JOIN)
    } {
      child.getToken.getType match {
        case TOK_TABREF =>
          tabrefCtr += 1
          if (tabrefCtr > 1) joinStr += BLANK + joinTypeStr
          joinStr += parseTableRef(child)
        case TOK_JOIN =>
          joinStr += BLANK + OPEN_PAREN + parseJoin(child).substring(1) + CLOSE_PAREN +
            BLANK + joinTypeStr
      }
    }

    // get Join condition
    for {
      child <- ast.getChildren.toList
      if child != null && child.getToken.getType == EQUAL
    } {
      joinStr += BLANK + SQL_ON
      val rootExpr = parseToExpr(ast)
      joinStr += getStrFromExpr(rootExpr)
    }

    joinStr
  }

  private def parseTableRef(ast: ASTNode): String = {
    var appendStr = EMPTY
    var db = EMPTY
    var table = EMPTY
    var alias = EMPTY
    for {
      child <- ast.getChildren.toList
      if child != null
    } {
      child.getToken.getType match {
        case TOK_TABNAME =>
          child.getChildren.toList match {
            case c1 :: c2 :: Nil =>
              db = c1.getText
              table = c2.getText
            case _ => table = child.getChild(0).getText
          }
        case _ => alias = child.getText
      }
    }

    appendStr += BLANK + (if (db.isEmpty) EMPTY
                          else
                            db + SL_DOT) + table + (if (alias.isEmpty)
                                                      EMPTY
                                                    else
                                                      BLANK + alias)
    appendStr
  }

  private def parseSelectExpr(ast: ASTNode): String = {
    var appendStr = EMPTY
    for {
      child <- ast.getChildren.toList
      if child != null && (child.getToken.getType == TOK_SELECT || child.getToken.getType == TOK_SELECTDI)
    } {
      if (child.getToken.getType == TOK_SELECTDI) appendStr += BLANK + SQL_DISTINCT
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
                case _ =>
                  throw SLUnsupportedSQLConstruct(
                    s"Supported children size: 1," +
                      s" but found ${child.getChildren.size}") // forced to throw error for unhandled cases
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
            case PLUS | MINUS | STAR | DIVIDE =>
              selFieldCtr += 1
              if (selFieldCtr > 1) appendStr += SL_COMMA
              var ctr = 0
              for {
                l4child <- l3child.getChildren.toList
              } {
                ctr += 1
                if (ctr > 1) appendStr += BLANK + l3child.getText
                l4child.getToken.getType match {
                  case PLUS | MINUS | STAR | DIVIDE =>
                    l3child.getToken.getType match {
                      case STAR | DIVIDE =>
                        appendStr += BLANK + OPEN_PAREN + parseOperator(l4child).substring(1) + CLOSE_PAREN
                      case _ => appendStr += parseOperator(l4child)
                    }
                  case _ => appendStr += parseOperator(l4child)
                }
              }
              appendStr
            case Identifier => appendStr += BLANK + SQL_AS + BLANK + l3child.getText
            case StringLiteral =>
              selFieldCtr += 1
              if (selFieldCtr > 1) appendStr += SL_COMMA
              appendStr += BLANK + l3child.getText
            case TOK_FUNCTION =>
              selFieldCtr += 1
              if (selFieldCtr > 1) appendStr += SL_COMMA
              val function = getBinaryVal(l3child)
              appendStr += getStrFromExpr(function)
            case TOK_FUNCTIONDI =>
              selFieldCtr += 1
              if (selFieldCtr > 1) appendStr += SL_COMMA
              val function = getBinaryVal(l3child).asInstanceOf[SqlFunctionDistinct]
              appendStr += getStrFromExpr(function)
            case _ => throw SLUnsupportedSQLConstruct(s"Unsupported Token Type: ${l3child.getToken.getType}") // forced to throw error for unhandled cases
          }
        }
      }
    }
    appendStr
  }

  private def parseOperator(ast: ASTNode): String = {
    var appendStr = EMPTY
    ast.getToken.getType match {
      case PLUS =>
        var ctr = 0
        for {
          child <- ast.getChildren.toList
        } {
          ctr += 1
          if (ctr > 1) appendStr += BLANK + ast.getText
          appendStr += parseOperator(child)
        }
      case StringLiteral => appendStr += BLANK + ast.getText
      case TOK_TABLE_OR_COL =>
        ast.getChildren.toList match {
          case c1 :: Nil =>
            appendStr += BLANK + c1.getToken.getText
          case _ =>
            throw SLUnsupportedSQLConstruct(
              s"Supported children size: 1," +
                s" but found ${ast.getChildren.size}") // forced to throw error for unhandled cases
        }
      case TOK_FUNCTION =>
        val function = getBinaryVal(ast)
        appendStr += getStrFromExpr(function)
    }

    appendStr
  }

  private def parseWhere(ast: ASTNode): String = {
    var whereStr = BLANK + SQL_WHERE
    val child1 = ast.getChild(0).asInstanceOf[ASTNode]
    if (child1.getToken.getType == TOK_SUBQUERY_EXPR)
      whereStr += parseAstToSql(child1)
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
      case TOK_SUBQUERY_EXPR => new SubQueryExpr(ast)
      case TOK_HAVING =>
        val root = new HavingExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          root.children += parseToExpr(child)
        }
        root
      case TOK_JOIN | TOK_LEFTOUTERJOIN | TOK_RIGHTOUTERJOIN | TOK_FULLOUTERJOIN=>
        val root = new JoinExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null && child.getToken.getType != TOK_TABREF && child.getToken.getType != TOK_JOIN &&
            child.getToken.getType != TOK_LEFTOUTERJOIN && child.getToken.getType != TOK_RIGHTOUTERJOIN &&
            child.getToken.getType != TOK_FULLOUTERJOIN
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
      case KW_NOT =>
        val notExpr = NotLogicalExpr(ListBuffer())
        for {
          child <- ast.getChildren.toList
          if child != null
        } {
          notExpr.children += parseToExpr(child)
        }
        notExpr
      case EQUAL | EQUAL_NS | GREATERTHAN | GREATERTHANOREQUALTO | LESSTHAN |
          LESSTHANOREQUALTO | NOTEQUAL | KW_LIKE =>
        ast.getChild(0).asInstanceOf[ASTNode].getToken.getType match {
          case TOK_TABLE_OR_COL           =>
            ast.getChild(1).asInstanceOf[ASTNode].getToken.getType match {
              case StringLiteral | KW_TRUE | KW_FALSE | Number => getColumnBinaryExpression(ast)
              case _ => getCompoundBinaryExpression(ast)
            }
          case TOK_FUNCTION | MINUS | DOT => getCompoundBinaryExpression(ast)
        }
      case TOK_FUNCTION =>
        getBinaryVal(ast)
      case _ => throw SLUnsupportedSQLConstruct(s"Unsupported Token Type: ${ast.getToken.getType}") // force to throw exception
    }
  }

  private def getCompoundBinaryExpression(ast: ASTNode): BinaryExpr = {
    ast.getChildren.toList match {
      case c1 :: c2 :: Nil =>
        val leftBinaryVal = getBinaryVal(c1)
        val rightBinaryVal = getBinaryVal(c2)
        BinaryExpr(leftBinaryVal, ast.getToken.getText, rightBinaryVal)
      case _ =>
        throw SLUnsupportedSQLConstruct(
          s"Supported children size: 2," +
            s" but found ${ast.getChildren.size}") // force to throw exception
    }
  }

  private def getBinaryVal(ast: ASTNode): BinaryVal = {
    ast.getToken.getType match {
      case TOK_FUNCTION =>
        val functionName = ast.getChild(0).getText
        functionName match {
          case "isnull" =>
            val params = ListBuffer[BinaryVal]()
            params += getBinaryVal(ast.getChild(1).asInstanceOf[ASTNode])
            SqlFunctionIsNull(functionName, params: _*)
          case "isnotnull" =>
            val params = ListBuffer[BinaryVal]()
            params += getBinaryVal(ast.getChild(1).asInstanceOf[ASTNode])
            SqlFunctionIsNotNull(functionName, params: _*)
          case "in" =>
            val leftVal = getBinaryVal(ast.getChild(1).asInstanceOf[ASTNode])
            val params = ListBuffer[BinaryVal]()
            for {
            child <- ast.getChildren.toList
            if child.childIndex > 1
            } {
              params += getBinaryVal(child)
            }
            SqlFunctionIn(functionName, leftVal, params: _*)
          case "between" =>
            val leftVal = getBinaryVal(ast.getChild(2).asInstanceOf[ASTNode])
            val params = ListBuffer[BinaryVal]()
            for {
              child <- ast.getChildren.toList
              if child.childIndex > 2
            } {
              params += getBinaryVal(child)
            }
            SqlFunctionBetween(functionName, leftVal, params: _*)
          case "WHEN" =>
            val params = ListBuffer[BinaryVal]()
            for {
              child <- ast.getChildren.toList
              if child.childIndex > 0
            } {
              params += getBinaryVal(child)
            }
            SqlFunctionCaseWhen(params: _*)
          case "TOK_DATE" =>
            SqlFunctionAsDate(ast.getChild(1).getText)
          case "TOK_TIMESTAMP" =>
            SqlFunctionAsTimestamp(ast.getChild(1).getText)
          case _ =>
            val params = ListBuffer[BinaryVal]()
            for {
              child <- ast.getChildren.toList
              if child.getChildIndex > 0
            } {
              params += getBinaryVal(child)
            }
            SqlFunction(functionName, params: _*)
        }

      case TOK_FUNCTIONDI =>
        val functionName = ast.getChild(0).getText
        val params = ListBuffer[BinaryVal]()
        params += getBinaryVal(ast.getChild(1).asInstanceOf[ASTNode])
        SqlFunctionDistinct(functionName, params: _*)
      case DOT =>
        val tableAlias = ast.getChild(0).getChild(0).getText
        val colName = ast.getChild(1).getText
        if (tableAlias.isEmpty) Column(colName)
        else Column(colName, Some(tableAlias))
      case TOK_TABLE_OR_COL =>
        ast.getChildren.toList match {
          case c1 :: Nil => Column(c1.getText)
          case _ =>
            throw SLUnsupportedSQLConstruct(s"Supported children size: 1," +
              s" but found ${ast.getChildren.size}") // force to throw exception
        }
      case Number        => NumberConstantExpr(ast.getText)
      case StringLiteral => StringConstantExpr(ast.getText)
      case MINUS | EQUAL | EQUAL_NS | GREATERTHAN | GREATERTHANOREQUALTO | LESSTHAN |
        LESSTHANOREQUALTO | NOTEQUAL=>
        val leftBinaryVal = getBinaryVal(ast.getChild(0).asInstanceOf[ASTNode])
        val rightBinaryVal = getBinaryVal(ast.getChild(1).asInstanceOf[ASTNode])
        BinaryExpr(leftBinaryVal, ast.getToken.getText, rightBinaryVal)
      case _ => throw SLUnsupportedSQLConstruct(s"Unsupported Token Type: ${ast.getToken.getType}") // force to throw exception
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
      case SubQueryExpr(ast) => parseToSql(ast)
      case NotLogicalExpr(children) =>
        var notStr = BLANK + SQL_NOT
        children.toList match {
          case c1 :: Nil => notStr += getStrFromExpr(c1)
          case _ =>
            notStr += OPEN_PAREN
            for {
              child <- children.toList
            } {
              notStr += getStrFromExpr(child)
            }
            notStr += CLOSE_PAREN
        }
        notStr
      case AndLogicalExpr(children) =>
        var andStr = EMPTY
        var counter = 0
        for {
          child <- children.toList
          if child != null
        } {
          counter += 1
          child match {
            case NotLogicalExpr(_) =>
              if (counter == 1)
                andStr += BLANK + getStrFromExpr(child).substring(1)
              else
                andStr += BLANK + SQL_AND + BLANK + getStrFromExpr(child).substring(1)
            case LogicalExpr(_) =>
              if (counter == 1)
                andStr += BLANK + OPEN_PAREN + getStrFromExpr(child).substring(
                  1) +
                  CLOSE_PAREN
              else
                andStr += BLANK + SQL_AND + BLANK + OPEN_PAREN + getStrFromExpr(
                  child).substring(1) +
                  CLOSE_PAREN
            case _: SqlFunctionBetween => andStr += BLANK + OPEN_PAREN + getStrFromExpr(child).substring(1) + CLOSE_PAREN
            case _ =>
              if (counter == 1) andStr += getStrFromExpr(child)
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
              if (counter == 1)
                orStr += BLANK + OPEN_PAREN + getStrFromExpr(child).substring(1) +
                  CLOSE_PAREN
              else
                orStr += BLANK + SQL_OR + BLANK + OPEN_PAREN + getStrFromExpr(
                  child).substring(1) +
                  CLOSE_PAREN
            case _ =>
              if (counter == 1) orStr += getStrFromExpr(child)
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
      case JoinExpr(children) =>
        var joinStr = EMPTY
        for {
          child <- children.toList
          if child != null
        } {
          joinStr += getStrFromExpr(child)
        }
        joinStr
      case BinaryExpr(leftVal, operator, rightVal) =>
        val leftStr = getStrFromExpr(leftVal)
        val rightStr = getStrFromExpr(rightVal)
        val bStr = leftStr + BLANK + operator + BLANK + rightStr.trim
//        // TODO make this one dynamic
//        rightVal match {
//          case expr: StringConstantExpr if leftVal.isInstanceOf[Column] =>
//            applyOptimization(bStr,
//                              leftVal.asInstanceOf[Column],
//                              operator,
//                              expr)
//          case _ => bStr
//        }
        bStr
      case Column(name, alias) =>
        val colStr = (if (alias.isEmpty) BLANK
                      else BLANK + alias.get + SL_DOT) + name
        colStr
      case SqlFunction(name, params @ _*) =>
        var funcStr = BLANK + name + OPEN_PAREN
        var paramCtr = 0
        for {
          binaryVal <- params
        } {
          paramCtr += 1
          if (paramCtr > 1) funcStr += SL_COMMA
          if (paramCtr == 1) funcStr += getStrFromExpr(binaryVal).substring(1)
          else funcStr += getStrFromExpr(binaryVal)
        }
        funcStr += CLOSE_PAREN
        funcStr
      case SqlFunctionDistinct(name, params @ _*) =>
        var funcStr = BLANK + name + OPEN_PAREN + SQL_DISTINCT + BLANK
        var paramCtr = 0
        for {
          binaryVal <- params
        } {
          paramCtr += 1
          if (paramCtr > 1) funcStr += SL_COMMA
          if (paramCtr == 1) funcStr += getStrFromExpr(binaryVal).substring(1)
          else funcStr += getStrFromExpr(binaryVal)
        }
        funcStr += CLOSE_PAREN
        funcStr
      case SqlFunctionIsNull(_, params @ _*) =>
        var funcStr = BLANK
        var paramCtr = 0
        for {
          binaryVal <- params
        } {
          paramCtr += 1
          if (paramCtr > 1) funcStr += SL_COMMA
          if (paramCtr == 1) funcStr += getStrFromExpr(binaryVal).substring(1)
          else funcStr += getStrFromExpr(binaryVal)
        }
        funcStr += BLANK + SQL_IS_NULL
        funcStr
      case SqlFunctionIsNotNull(_, params @ _*) =>
        var funcStr = BLANK
        var paramCtr = 0
        for {
          binaryVal <- params
        } {
          paramCtr += 1
          if (paramCtr > 1) funcStr += SL_COMMA
          if (paramCtr == 1) funcStr += getStrFromExpr(binaryVal).substring(1)
          else funcStr += getStrFromExpr(binaryVal)
        }
        funcStr += BLANK + SQL_IS_NOT_NULL
        funcStr
      case SqlFunctionIn(_, leftVal, params @ _*) =>
        var funcStr = getStrFromExpr(leftVal) + BLANK + SQL_IN + BLANK + OPEN_PAREN
        var paramCtr = 0
        for {
          binaryVal <- params
        } {
          paramCtr += 1
          if (paramCtr > 1) funcStr += SL_COMMA
          if (paramCtr == 1) funcStr += getStrFromExpr(binaryVal).substring(1)
          else funcStr += getStrFromExpr(binaryVal)
        }
        funcStr += CLOSE_PAREN
        funcStr
      case SqlFunctionBetween(_, leftVal, params @ _*) =>
        var funcStr = getStrFromExpr(leftVal) + BLANK + SQL_BETWEEN
        var paramCtr = 0
        for {
          binaryVal <- params
        } {
          paramCtr += 1
          if (paramCtr > 1) funcStr += BLANK + SQL_AND
          if (paramCtr == 1) funcStr += getStrFromExpr(binaryVal)
          else funcStr += getStrFromExpr(binaryVal)
        }
        funcStr
      case SqlFunctionCaseWhen(params @ _*) =>
        var funcStr = BLANK + SQL_CASE
        val size = params.size
        val endKW = if (size > 2) SQL_ELSE else SQL_THEN
        var paramCtr = 0
        for {
          binaryVal <- params
        } {
          paramCtr += 1
          if (paramCtr == size)
            funcStr += BLANK + endKW + getStrFromExpr(binaryVal)
          else
            if (paramCtr % 2 == 0) funcStr += BLANK  + SQL_THEN + getStrFromExpr(binaryVal)
            else funcStr += BLANK  + SQL_WHEN + getStrFromExpr(binaryVal)

        }
        funcStr += BLANK + SQL_END
        funcStr
      case SqlFunctionAsDate(date) =>
        BLANK + SQL_CAST + OPEN_PAREN + date + BLANK + SQL_AS + BLANK + SQL_DATE + CLOSE_PAREN
      case SqlFunctionAsTimestamp(date) =>
        BLANK + SQL_CAST + OPEN_PAREN + date + BLANK + SQL_AS + BLANK + SQL_TIMESTAMP + CLOSE_PAREN
      case StringConstantExpr(value) => BLANK + value
      case NumberConstantExpr(value) => BLANK + value.toString
      case _ => throw SLUnsupportedSQLConstruct(s"Unsupported Expression: $rootExpr")  // forced to throw error for unhandled cases for now
    }
  }

  private def applyOptimization(
      bStr: String,
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

  private def parseGroupBy(ast: ASTNode): String = {
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
            case DOT              => groupByStr += parseGroupBy(child)
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
            case _ => throw SLUnsupportedSQLConstruct(s"Unsupported Token Type: ${child.getToken.getType}") // force to throw exception for now
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
          case _ =>
            throw SLUnsupportedSQLConstruct(
              s"Supported children size: 1," +
                s" but found ${ast.getChildren.size}") // forced to throw error for unhandled cases
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
          case _ =>
            throw SLUnsupportedSQLConstruct(
              s"Supported children size: 1," +
                s" but found ${ast.getChildren.size}") // forced to throw error for unhandled cases
        }
      case _ => throw SLUnsupportedSQLConstruct(s"Unsupported Token Type: ${ast.getToken.getType}") // force to throw exception for now
    }
    groupByStr
  }

  private def parseHaving(ast: ASTNode): String = {
    var havingStr = BLANK + SQL_HAVING
    val rootExpr = parseToExpr(ast)
    havingStr += getStrFromExpr(rootExpr)
    havingStr
  }

  private def parseOrderBy(ast: ASTNode): String = {
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
            case _ => throw SLUnsupportedSQLConstruct(s"Unsupported Token Type: ${child.getToken.getType}") // force to throw exception for now
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
          case _ =>
            throw SLUnsupportedSQLConstruct(
              s"Supported children size: 1," +
                s" but found ${ast.getChildren.size}") // forced to throw error for unhandled cases
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
          case _ =>
            throw SLUnsupportedSQLConstruct(
              s"Supported children size: 2," +
                s" but found ${ast.getChildren.size}") // forced to throw error for unhandled cases
        }
      case TOK_FUNCTION =>
        val function = getBinaryVal(ast)
        orderByStr += getStrFromExpr(function)
      case _ => throw SLUnsupportedSQLConstruct(s"Unsupported Token Type: ${ast.getToken.getType}") // force to throw exception for now
    }
    orderByStr
  }

  private def sanitizeSQLInput(sql: String): String = {
    var sanitized = sql
    val pattern = "`(.*?)`".r
    val matches = pattern.findAllIn(sql).toList
    for {
      s <- matches
    } {
      sanitized = sanitized.replaceAll(s, BT_REPLACE_START + s.substring(1, s.length - 1)
        .replaceAll(" ", BT_REPLACE_SPACE) + BT_REPLACE_END)
    }
    sanitized
  }

  private def sanitizeSQLOutput(sql: String): String = {
    var sanitized = sql
    val pattern = (BT_REPLACE_START + "(.*?)" + BT_REPLACE_END).r
    val matches = pattern.findAllIn(sql).toList
    for {
      s <- matches
    } {
      sanitized = sanitized.replaceAll(s, HIVE_ACC_QUOTE +
        s.substring(BT_REPLACE_START.length, s.length - BT_REPLACE_END.length)
        .replaceAll(BT_REPLACE_SPACE, BLANK) + HIVE_ACC_QUOTE)
    }
    sanitized
  }

  private def parseLimit(ast: ASTNode): String = {
    BLANK + SQL_LIMIT + BLANK + ast.getChild(0).getText
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

  case class NotLogicalExpr(override val children: ListBuffer[Expr])
    extends LogicalExpr(children)

  class HavingExpr(val children: ListBuffer[Expr]) extends Expr

  object HavingExpr {
    def unapply(arg: HavingExpr): Option[ListBuffer[Expr]] = Some(arg.children)
  }

  class JoinExpr(val children: ListBuffer[Expr]) extends Expr

  object JoinExpr {
    def unapply(arg: JoinExpr): Option[ListBuffer[Expr]] = Some(arg.children)
  }

  class SubQueryExpr(val ast: ASTNode) extends Expr

  object SubQueryExpr {
    def unapply(arg: SubQueryExpr): Option[ASTNode] = Some(arg.ast)
  }

  case class Table(name: String,
                   db: Option[String] = None,
                   alias: Option[String] = None)
      extends Expr

  sealed trait BinaryVal extends Expr

  case class Column(name: String, alias: Option[String] = None)
      extends BinaryVal

  case class SqlFunction(funcName: String, params: BinaryVal*) extends BinaryVal

  case class SqlFunctionDistinct(funcName: String, params: BinaryVal*) extends BinaryVal

  case class SqlFunctionIsNull(funcName: String, params: BinaryVal*) extends BinaryVal

  case class SqlFunctionIsNotNull(funcName: String, params: BinaryVal*) extends BinaryVal

  case class SqlFunctionIn(funcName: String, leftVal: BinaryVal, params: BinaryVal*) extends BinaryVal

  case class SqlFunctionBetween(funcName: String, leftVal: BinaryVal, params: BinaryVal*) extends BinaryVal

  case class SqlFunctionCaseWhen(params: BinaryVal*) extends BinaryVal

  case class SqlFunctionAsDate(date: String) extends BinaryVal

  case class SqlFunctionAsTimestamp(date: String) extends BinaryVal

  case class WhenThenElement(whenCondition: BinaryVal, thenCondition: BinaryVal) extends BinaryVal

  case class BinaryExpr(leftVal: BinaryVal,
                        operator: String,
                        rightVal: BinaryVal)
      extends BinaryVal

  sealed trait SelectExpr extends Expr

  case class SelectAll(db: Option[String] = None) extends SelectExpr

  case class SelectField(column: Column) extends SelectExpr

  case class StringConstantExpr(value: String) extends BinaryVal

  case class NumberConstantExpr(value: String) extends BinaryVal

  case class HiveQuery(original: String, suggested: String)

  case class OptimizeResult(original: String,
                            optimized: Boolean,
                            result: String,
                            errorOpt: Option[Throwable])
}
