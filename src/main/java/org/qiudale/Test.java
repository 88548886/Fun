package org.qiudale;

import java.util.ArrayList;
import java.util.List;

import org.qiudale.jsql.context.SqlContext;
import org.qiudale.jsql.function.Function;
import org.qiudale.jsql.function.VoidFunction;
import org.qiudale.jsql.rdd.Row;
import org.qiudale.vo.Foo;

public class Test {
	public static void main(String[] args) throws Exception {

		List<Foo> fooList = new ArrayList<Foo>();
		fooList.add(new Foo().setfoofield1("f11").setfoofield2("f22").setfoofield3("f33"));
		fooList.add(new Foo().setfoofield1("f11").setfoofield2("fab").setfoofield3("fcd"));
		fooList.add(new Foo().setfoofield1("f11").setfoofield2("xd").setfoofield3("fxw"));
		fooList.add(new Foo().setfoofield1("f44").setfoofield2("f55").setfoofield3("f66"));
		SqlContext.createTempView(fooList.iterator(), new Function<Foo, Row>() {
			@Override
			public Row apply(Foo item) {
				Row row = new Row();
				row.addColumn("foofield1", item.getfoofield1());
				row.addColumn("foofield2", item.getfoofield2());
				row.addColumn("foofield3", item.getfoofield3());
				return row;
			}
		}).sql("select * from dual where foofield1 = 'f11'").sql("select * from dual where foofield2='fab'")
				.toRdd(new Function<Row, Foo>() {
					@Override
					public Foo apply(Row item) {
						return new Foo().setfoofield1(item.getAsString("foofield1"))
								.setfoofield2(item.getAsString("foofield2"))
								.setfoofield3(item.getAsString("foofield3"));
					}
				}).foreach(new VoidFunction<Foo>() {
					@Override
					public void apply(Foo foo) {
						System.out.println(foo.getfoofield2());
					}
				});
	}
}
