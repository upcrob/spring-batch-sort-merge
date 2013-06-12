package com.upcrob.example.spring.batch;


import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.core.io.FileSystemResource;

import com.upcrob.spring.batch.tasklet.FlatFileItemIoFactory;

/**
 * Creates readers and writers for flat files that contain records
 * that represent Person objects.
 */
public class PersonBeanIoFactory implements FlatFileItemIoFactory<PersonBean> {

    @Override
    public FlatFileItemReader<PersonBean> getReader(FileSystemResource r) {
	FlatFileItemReader<PersonBean> reader = new FlatFileItemReader<PersonBean>();
	reader.setResource(r);
	reader.setLineMapper(new PersonLineMapper());
	return reader;
    }

    @Override
    public FlatFileItemWriter<PersonBean> getWriter(FileSystemResource r) {
	FlatFileItemWriter<PersonBean> writer = new FlatFileItemWriter<PersonBean>();
	writer.setResource(r);
	writer.setLineAggregator(new PersonLineAggregator());
	return writer;
    }
    
}
