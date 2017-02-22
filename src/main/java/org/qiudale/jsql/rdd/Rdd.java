package org.qiudale.jsql.rdd;

import java.util.ArrayList;
import java.util.List;

import org.qiudale.jsql.function.Function;
import org.qiudale.jsql.function.VoidFunction;

public class Rdd<G> {
	private List<G> collection;

	public Rdd() {
		collection = new ArrayList<G>();
	}

	public <T> Rdd<T> map(Function<G, T> f) {
		Rdd<T> rdd = new Rdd<T>();
		for (G item : collection) {
			T apply = f.apply(item);
			rdd.addItem(apply);
		}
		return rdd;
	}

	public void foreach(VoidFunction<G> f) {
		for (G item : collection) {
			f.apply(item);
		}
	}

	public Rdd<G> filter(Function<G, Boolean> f) {
		Rdd<G> temp = new Rdd<G>();
		for (G item : collection) {
			boolean apply = f.apply(item);
			if (apply) {
				temp.addItem(item);
			}
		}
		return temp;
	}

	public void addItem(G item) {
		collection.add(item);
	}
}
