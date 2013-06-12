package com.upcrob.example.spring.batch;

import org.springframework.batch.item.file.LineMapper;

public class PersonLineMapper implements LineMapper<PersonBean> {

	@Override
	public PersonBean mapLine(String line, int lineNumber) throws Exception {
		PersonBean bean = new PersonBean();
		String[] ls = line.split(",");
		bean.setId(Integer.parseInt(ls[0]));
		bean.setName(ls[1]);
		return bean;
	}

}
