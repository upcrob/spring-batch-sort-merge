package com.upcrob.example.spring.batch;

import java.util.Comparator;

/**
 * Simple comparator that compares two Person objects by name.
 */
public class PersonComparator implements Comparator<PersonBean> {
	@Override
	public int compare(PersonBean a, PersonBean b) {
	    if (a == null)
	        throw new IllegalArgumentException("argument object a cannot be null");
	    if (b == null)
	        throw new IllegalArgumentException("argument object b cannot be null");
		return a.getName().compareTo(b.getName());
	}
}
