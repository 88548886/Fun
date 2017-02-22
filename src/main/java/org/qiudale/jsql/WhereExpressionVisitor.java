package org.qiudale.jsql;

import org.qiudale.jsql.rdd.Row;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnalyticExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.CastExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.ExtractExpression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.HexValue;
import net.sf.jsqlparser.expression.IntervalExpression;
import net.sf.jsqlparser.expression.JdbcNamedParameter;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.JsonExpression;
import net.sf.jsqlparser.expression.KeepExpression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.MySQLGroupConcat;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.NumericBind;
import net.sf.jsqlparser.expression.OracleHierarchicalExpression;
import net.sf.jsqlparser.expression.OracleHint;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.RowConstructor;
import net.sf.jsqlparser.expression.SignedExpression;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.UserVariable;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.WithinGroupExpression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Modulo;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.expression.operators.relational.RegExpMatchOperator;
import net.sf.jsqlparser.expression.operators.relational.RegExpMySQLOperator;
import net.sf.jsqlparser.statement.select.SubSelect;

public class WhereExpressionVisitor implements ExpressionVisitor {
	protected Expression expression;
	protected Row rowData;
	protected boolean result;

	public WhereExpressionVisitor(Row rowData, Expression expression) {
		this.rowData = rowData;
		this.expression = expression;
		this.result = true;
	}

	public void visit(NullValue nullValue) {
		System.out.println("visit NullValue:" + nullValue);
	}

	public void visit(Function function) {
		System.out.println("visit Function:" + function);
	}

	public void visit(SignedExpression signedExpression) {
		System.out.println("visit SignedExpression:" + signedExpression);

	}

	public void visit(JdbcParameter jdbcParameter) {

		System.out.println("visit JdbcParameter:" + jdbcParameter);
	}

	public void visit(JdbcNamedParameter jdbcNamedParameter) {

		System.out.println("visit JdbcNamedParameter:" + jdbcNamedParameter);
	}

	public void visit(DoubleValue doubleValue) {

		System.out.println("visit DoubleValue:" + doubleValue);
	}

	public void visit(LongValue longValue) {
		System.out.println("visit LongValue:" + longValue);

	}

	public void visit(HexValue hexValue) {
		System.out.println("visit HexValue:" + hexValue);

	}

	public void visit(DateValue dateValue) {

		System.out.println("visit DateValue:" + dateValue);
	}

	public void visit(TimeValue timeValue) {

		System.out.println("visit TimeValue:" + timeValue);
	}

	public void visit(TimestampValue timestampValue) {

		System.out.println("visit TimestampValue:" + timestampValue);
	}

	public void visit(Parenthesis parenthesis) {
		System.out.println("visit Parenthesis:" + parenthesis);

	}

	public void visit(StringValue stringValue) {
		System.out.println("visit StringValue:" + stringValue);

	}

	public void visit(Addition addition) {
		System.out.println("visit Addition:" + addition);

	}

	public void visit(Division division) {
		System.out.println("visit Division:" + division);

	}

	public void visit(Multiplication multiplication) {

		System.out.println("visit Multiplication:" + multiplication);
	}

	public void visit(Subtraction subtraction) {

		System.out.println("visit Subtraction:" + subtraction);
	}

	public void visit(AndExpression andExpression) {
		if (!result) return;
		Expression leftExpression = andExpression.getLeftExpression();
		leftExpression.accept(this);
		Expression rightExpression = andExpression.getRightExpression();
		rightExpression.accept(this);
	}

	public void visit(OrExpression orExpression) {

		System.out.println("visit OrExpression:" + orExpression);
	}

	public void visit(Between between) {

		System.out.println("visit Between:" + between);
	}

	public void visit(EqualsTo equalsTo) {
		if (!result) return;
		String columnName = equalsTo.getLeftExpression().toString();
		String requiredValue = equalsTo.getRightExpression().toString();
		Object cellValue = rowData.getRaw(columnName);
		if (cellValue instanceof String) {
			result = cellValue.equals(requiredValue.replace("'", ""));
		} else if (cellValue instanceof Integer) {
			result = ((Integer) cellValue).equals(Integer.parseInt(requiredValue));
		}

	}

	public void visit(GreaterThan greaterThan) {
		if (!result) return;
		String columnName = greaterThan.getLeftExpression().toString();
		String requiredValue = greaterThan.getRightExpression().toString();
		Object cellValue = rowData.getRaw(columnName);
		if (cellValue instanceof String) {
			result = cellValue.equals(requiredValue.replace(",", ""));
		} else if (cellValue instanceof Integer) {
			result = ((Integer) cellValue) > Integer.parseInt(requiredValue);
		}
	}

	public void visit(GreaterThanEquals greaterThanEquals) {
		if (!result) return;
		String columnName = greaterThanEquals.getLeftExpression().toString();
		String requiredValue = greaterThanEquals.getRightExpression().toString();
		Object cellValue = rowData.getRaw(columnName);
		if (cellValue instanceof String) {
			result = ((String) cellValue).compareTo(requiredValue.replace(",", "")) > 0 ? true : false;
		} else if (cellValue instanceof Integer) {
			result = ((Integer) cellValue) > Integer.parseInt(requiredValue);
		}
	}

	public void visit(InExpression inExpression) {
		System.out.println("visit InExpression:" + inExpression);

	}

	public void visit(IsNullExpression isNullExpression) {
		System.out.println("visit IsNullExpression:" + isNullExpression);

	}

	public void visit(LikeExpression likeExpression) {

		System.out.println("visit LikeExpression:" + likeExpression);
	}

	public void visit(MinorThan minorThan) {

		System.out.println("visit MinorThan:" + minorThan);
	}

	public void visit(MinorThanEquals minorThanEquals) {

		System.out.println("visit MinorThanEquals:" + minorThanEquals);
	}

	public void visit(NotEqualsTo notEqualsTo) {
		System.out.println("visit NotEqualsTo:" + notEqualsTo);

	}

	public void visit(net.sf.jsqlparser.schema.Column tableColumn) {

		System.out.println("visit Column:" + tableColumn);
	}

	public void visit(SubSelect subSelect) {

		System.out.println("visit SubSelect:" + subSelect);
	}

	public void visit(CaseExpression caseExpression) {
		System.out.println("visit CaseExpression:" + caseExpression);

	}

	public void visit(WhenClause whenClause) {
		System.out.println("visit Function:" + whenClause);

	}

	public void visit(ExistsExpression existsExpression) {
		System.out.println("visit Function:" + existsExpression);

	}

	public void visit(AllComparisonExpression allComparisonExpression) {
		System.out.println("visit Function:" + allComparisonExpression);

	}

	public void visit(AnyComparisonExpression anyComparisonExpression) {

		System.out.println("visit Function:" + anyComparisonExpression);
	}

	public void visit(Concat concat) {

		System.out.println("visit Concat:" + concat);
	}

	public void visit(Matches matches) {
		System.out.println("visit Matches:" + matches);

	}

	public void visit(BitwiseAnd bitwiseAnd) {
		System.out.println("visit BitwiseAnd:" + bitwiseAnd);

	}

	public void visit(BitwiseOr bitwiseOr) {
		System.out.println("visit BitwiseOr:" + bitwiseOr);

	}

	public void visit(BitwiseXor bitwiseXor) {
		System.out.println("visit BitwiseXor:" + bitwiseXor);

	}

	public void visit(CastExpression cast) {
		System.out.println("visit CastExpression:" + cast);

	}

	public void visit(Modulo modulo) {
		System.out.println("visit Modulo:" + modulo);

	}

	public void visit(AnalyticExpression aexpr) {

		System.out.println("visit AnalyticExpression:" + aexpr);
	}

	public void visit(WithinGroupExpression wgexpr) {

		System.out.println("visit WithinGroupExpression:" + wgexpr);
	}

	public void visit(ExtractExpression eexpr) {
		System.out.println("visit ExtractExpression:" + eexpr);

	}

	public void visit(IntervalExpression iexpr) {

		System.out.println("visit IntervalExpression:" + iexpr);
	}

	public void visit(OracleHierarchicalExpression oexpr) {
		System.out.println("visit OracleHierarchicalExpression:" + oexpr);

	}

	public void visit(RegExpMatchOperator rexpr) {
		System.out.println("visit RegExpMatchOperator:" + rexpr);

	}

	public void visit(JsonExpression jsonExpr) {
		System.out.println("visit JsonExpression:" + jsonExpr);

	}

	public void visit(RegExpMySQLOperator regExpMySQLOperator) {
		System.out.println("visit RegExpMySQLOperator:" + regExpMySQLOperator);

	}

	public void visit(UserVariable var) {
		System.out.println("visit UserVariable:" + var);

	}

	public void visit(NumericBind bind) {
		System.out.println("visit NumericBind:" + bind);

	}

	public void visit(KeepExpression aexpr) {
		System.out.println("visit KeepExpression:" + aexpr);

	}

	public void visit(MySQLGroupConcat groupConcat) {
		System.out.println("visit MySQLGroupConcat:" + groupConcat);
	}

	public void visit(RowConstructor rowConstructor) {
		System.out.println("visit RowConstructor:" + rowConstructor);

	}

	public void visit(OracleHint hint) {
		System.out.println("visit OracleHint:" + hint);
	}

	public boolean eval(Row rowData) {
		this.rowData = rowData;
		expression.accept(this);
		return result;
	}
}
