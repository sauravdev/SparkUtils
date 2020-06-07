package com.saurav.Utilities;

import java.util.Enumeration;
import java.util.Properties;
import java.util.Vector;

@SuppressWarnings("serial")
public class OrderedProperties extends Properties {

	@SuppressWarnings("rawtypes")
	public OrderedProperties() {
		super();
		_names = new Vector();
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Enumeration propertyNames() {
		return _names.elements();
	}

	@SuppressWarnings("unchecked")
	public Object put(Object key, Object value) {
		if (_names.contains(key)) {
			_names.remove(key);
		}

		_names.add(key);

		return super.put(key, value);
	}

	public Object remove(Object key) {
		_names.remove(key);

		return super.remove(key);
	}

	@SuppressWarnings("rawtypes")
	private Vector _names;

}