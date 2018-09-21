package com.esoft.dp.spring;

public abstract class InstanceFactory {

	public Object createInstance() {
		return newInstance().getInstance();
	}

	public abstract Instance newInstance();

}
