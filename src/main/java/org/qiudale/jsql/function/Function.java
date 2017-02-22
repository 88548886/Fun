package org.qiudale.jsql.function;

public abstract class Function<Z,T> {
	public abstract T apply(Z item);
}
