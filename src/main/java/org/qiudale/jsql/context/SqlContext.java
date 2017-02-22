package org.qiudale.jsql.context;

import java.util.Iterator;

import org.qiudale.jsql.function.Function;
import org.qiudale.jsql.rdd.DataFrame;
import org.qiudale.jsql.rdd.Row;

public class SqlContext {
	public static <G> DataFrame createTempView(Iterator<G> iter, Function<G, Row> f) {
		DataFrame dataFrame = new DataFrame();
		while (iter.hasNext()) {
			G item = iter.next();
			Row row = f.apply(item);
			dataFrame.addItem(row);
		}
		return dataFrame;
	}
}
