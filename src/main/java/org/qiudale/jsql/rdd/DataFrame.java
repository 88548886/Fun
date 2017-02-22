package org.qiudale.jsql.rdd;

import java.io.StringReader;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import org.qiudale.jsql.WhereExpressionVisitor;
import org.qiudale.jsql.function.Function;

public class DataFrame {
	protected Rdd<Row> collection = new Rdd<Row>();

	public DataFrame addItem(Row item) {
		collection.addItem(item);
		return this;
	}

	@SuppressWarnings("unused")
	public DataFrame sql(final String sql) throws Exception {
		Statement stat = new CCJSqlParserManager().parse(new StringReader(sql));
		Select select = (Select) stat;
		PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
		// where表达
		final Expression expression = plainSelect.getWhere();
		// TODO 输出列考虑聚合函数count,avg,max,min,分组函数
		final List<SelectItem> outColumns = plainSelect.getSelectItems();
		collection = collection.filter(new Function<Row, Boolean>() {
			@Override
			public Boolean apply(Row row) {
				WhereExpressionVisitor visitor = new WhereExpressionVisitor(row, expression);
				return visitor.eval(row);
			}
		});
		return this;
	}

	public <T> Rdd<T> toRdd(Function<Row, T> f) {
		return collection.map(f);
	}
}
