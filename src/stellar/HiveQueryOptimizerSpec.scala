package stellar

import scala.util.Success

/**
  * <p>Test cases for Parsing Hive queries
  * Divided into these sections:
  * </p>
  * <ul>
  *   <li>#1 SIMPLE QUERIES TEST CASES</li>
  *   <li>#2 SELECT FIELDS TEST CASES</li>
  *   <li>#3 GREATER THAN LESS THAN OPERATORS</li>
  *   <li>#4 OR and AND OPERATORS W/ PARENTHESIS/GROUPING</li>
  *   <li>#5 ORDER BY TEST CASES</li>
  *   <li>#6 GROUP BY and HAVING TEST CASES<li>
  *   <li>#7 SUBQUERIES TEST CASES<li>
  * </ul>
  */
class HiveQueryOptimizerSpec extends FlatSpec with Matchers with Inside {
  behavior of "HiveQueryOptimizer"


  /*********************************** #1 SIMPLE QUERIES TEST CASES ********************************************/

  it should "optimize simple queries" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND test = true
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND M.last_digits = '01'
             | AND test = true
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries2" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
             | AND M.last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries without table alias" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member WHERE test = true AND member_id = 'M-000000001'
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member WHERE test = true AND member_id = 'M-000000001'
             | AND last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  /*********************************** #2 SELECT FIELDS TEST CASES **************************************************/

  it should "optimize simple queries with single select field" in {
    val sql =
      s"""
         |SELECT member_id FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT member_id FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
             | AND M.last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries with multiple select field" in {
    val sql =
      s"""
         |SELECT member_id, first_name, email FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT member_id, first_name, email FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
             | AND M.last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries with multiple select field combined with select *" in {
    val sql =
      s"""
         |SELECT *, member_id, first_name, email FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT *, member_id, first_name, email FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
             | AND M.last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries with multiple select field combined with select * with table alias" in {
    val sql =
      s"""
         |SELECT m.*, m.member_id, m.first_name, m.email FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT m.*, m.member_id, m.first_name, m.email FROM growingtree.member M WHERE test = true AND
             | M.member_id = 'M-000000001'
             | AND M.last_digits = '01'
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  /*********************************** #3 GREATER THAN LESS THAN OPERATORS *******************************************/

  it should "optimize simple queries with greater than operator" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND M.age_yrs > 18
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
             | AND M.last_digits = '01' AND M.age_yrs > 18
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries with less than operator" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND M.age_yrs < 18
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
             | AND M.last_digits = '01' AND M.age_yrs < 18
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries with less than or equal operator" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND M.age_yrs <= 18
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
             | AND M.last_digits = '01' AND M.age_yrs <= 18
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries with greater than or equal operator" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND M.age_yrs >= 18
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
             | AND M.last_digits = '01' AND M.age_yrs >= 18
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries with greater than operator in WHERE clause" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE age(birthdate) > 2
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE age(birthdate) > 2
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  /******************************* #4 OR and AND OPERATORS W/ PARENTHESIS/GROUPING ************************************/

  it should "optimize simple queries with OR operator" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND M.age_yrs >= 18 or test = true
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE (M.member_id = 'M-000000001'
             | AND M.last_digits = '01' AND M.age_yrs >= 18) OR test = true
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries with OR operator and AND enclosed in parenthesis" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE (M.member_id = 'M-000000001' AND M.age_yrs >= 18) or test = true
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE (M.member_id = 'M-000000001'
             | AND M.last_digits = '01' AND M.age_yrs >= 18) OR test = true
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries with AND operator and OR enclosed in parenthesis" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001' AND (M.age_yrs >= 18 or test = true)
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE M.member_id = 'M-000000001'
             | AND M.last_digits = '01' AND (M.age_yrs >= 18 OR test = true)
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize simple queries with AND operator and OR proper grouping" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE (M.member_id = 'M-000000001' AND M.test_field = 1) OR
         | (M.age_yrs >= 18 AND test = true)
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE (M.member_id = 'M-000000001' AND M.last_digits = '01'
             | AND M.test_field = 1) OR (M.age_yrs >= 18 AND test = true)
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  /*********************************** #5 ORDER BY TEST CASES ******************************************************/



  it should "optimize queries with order by" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by last_name
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
             | AND M.last_digits = '01' ORDER BY last_name ASC
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize queries with order by asc" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by last_name asc
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
             | AND M.last_digits = '01' ORDER BY last_name ASC
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize queries with order by desc" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by last_name desc
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
             | AND M.last_digits = '01' ORDER BY last_name DESC
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize queries with order by of multiple columns" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by last_name, first_name
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
             | AND M.last_digits = '01' ORDER BY last_name ASC, first_name ASC
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize queries with order by of multiple columns different sort per column" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by last_name asc,
         | first_name desc
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
             | AND M.last_digits = '01' ORDER BY last_name ASC, first_name DESC
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize queries with order by with table alias" in {
    val sql =
      s"""
         |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001' order by m.last_name asc,
         | m.first_name desc
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM growingtree.member M WHERE test = true AND M.member_id = 'M-000000001'
             | AND M.last_digits = '01' ORDER BY m.last_name ASC, m.first_name DESC
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  /*********************************** #6 GROUP BY and HAVING TEST CASES ******************************************************/

  it should "optimize queries with group by" in {
    val sql =
      s"""
         |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
         | AND M.member_id = 'M-000000001' group by m.age
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
             | AND M.member_id = 'M-000000001' AND M.last_digits = '01' GROUP BY m.age
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize queries with group by and Order by" in {
    val sql =
      s"""
         |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
         | AND M.member_id = 'M-000000001' group by m.age order by m.age
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
             | AND M.member_id = 'M-000000001' AND M.last_digits = '01' GROUP BY m.age ORDER BY m.age ASC
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize queries with group by and Order by desc" in {
    val sql =
      s"""
         |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
         | AND M.member_id = 'M-000000001' group by m.age order by m.age desc
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT M.member_id, M.last_name, M.age FROM growingtree.member M WHERE test = true
             | AND M.member_id = 'M-000000001' AND M.last_digits = '01' GROUP BY m.age ORDER BY m.age DESC
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize queries with group by and Order by desc no alias" in {
    val sql =
      s"""
         |SELECT member_id, last_name, age FROM growingtree.member WHERE test = true
         | AND member_id = 'M-000000001' group by age order by age desc
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT member_id, last_name, age FROM growingtree.member WHERE test = true
             | AND member_id = 'M-000000001' AND last_digits = '01' GROUP BY age ORDER BY age DESC
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  it should "optimize queries with group by and having" in {
    val sql =
      s"""
         |SELECT member_id, last_name, age FROM growingtree.member WHERE test = true
         | AND member_id = 'M-000000001' GROUP BY age HAVING MAX(field1) - MIN(field2) > 10
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT member_id, last_name, age FROM growingtree.member WHERE test = true
             | AND member_id = 'M-000000001' AND last_digits = '01' GROUP BY age HAVING MAX(field1) - MIN(field2) > 10
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  /*********************************** #7 SUBQUERIES TEST CASES ******************************************************/


  it should "optimize simple subqueries" in {
    val sql =
      s"""
         |SELECT * FROM member WHERE member_id in(select member_id from activity WHERE test = true)
       """.stripMargin
    val result = HiveQueryOptimizer.optimize(sql)

    inside(result) {
      case Success(sqlStr) =>
        sqlStr should equal(
          s"""
             |SELECT * FROM member WHERE member_id IN(SELECT member_id FROM activity WHERE test = true)
           """.stripMargin.replaceAll("\n", "").trim)
    }
  }

  // TODO: JOIN tests
  // TODO: Cross-JOIN tests
  // TODO: JOIN tests with subquery as tables.
}
