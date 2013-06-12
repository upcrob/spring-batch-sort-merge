package com.upcrob.example.spring.batch;

import java.io.Serializable;

/**
 * Simple bean that describes a Person with a name and id.
 */
public class PersonBean implements Comparable<PersonBean>, Serializable {
	
	private static final long serialVersionUID = 2417414020645506953L;
	
	private int id;
	private String name;
	
	public PersonBean() {
		id = 0;
		name = "NO NAME";
	}
	
	public PersonBean(int id, String name) {
		this.id = id;
		this.name = name;
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
	@Override
	public String toString() {
		return id + " | " + name;
	}

	@Override
	public int compareTo(PersonBean b) {
		return this.getName().compareTo(b.getName());
	}
}
