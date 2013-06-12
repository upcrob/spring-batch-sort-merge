package com.upcrob.example.spring.batch;

import org.springframework.batch.item.file.transform.LineAggregator;

public class PersonLineAggregator implements LineAggregator<PersonBean> {

	@Override
	public String aggregate(PersonBean bean) {
		return bean.getId() + "," + bean.getName();
	}

}
