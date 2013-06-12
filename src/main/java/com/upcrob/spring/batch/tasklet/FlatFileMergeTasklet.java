package com.upcrob.spring.batch.tasklet;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.repeat.RepeatStatus;

/**
 * Tasklet merges the contents of multiple input files into a designated output file
 * using a comparator.  As a precondition, this Tasklet assumes that each input file
 * is already in sorted order.
 * 
 * @author Rob Upcraft
 * 
 * @param <T> Type of object to be read / written.
 */
public class FlatFileMergeTasklet<T> implements Tasklet {

	private List<FlatFileItemReader<T>> readers;
	private FlatFileItemWriter<T> writer;
	private Comparator<T> comparator;
	private static final int DEFAULT_MAX_ROWS_READ = 100000;
	private int maxRecords;
	private static final Log logger = LogFactory.getLog(FlatFileMergeTasklet.class);
	
	public FlatFileMergeTasklet() {
		readers = null;
		maxRecords = DEFAULT_MAX_ROWS_READ;
		comparator = null;
	}
	
	public void setWriter(FlatFileItemWriter<T> writer) {
		this.writer = writer;
	}
	
	public void setReaders(List<FlatFileItemReader<T>> readers) {
		this.readers = readers;
	}
	
	public void setComparator(Comparator<T> c) {
		this.comparator = c;
	}
	
	/**
	 * Specifies how many records may be loaded into main memory without
	 * being written out to the destination file.  If OutOfMemory errors
	 * manifest themselves, this value should be decreased.
	 * @param maxLoadedRecords Maximum number of loaded records.
	 */
	public void setMaxLoadedRecords(int maxLoadedRecords) {
		if (maxLoadedRecords < 1)
			throw new IllegalArgumentException("max loaded records must be greater than 0");
		this.maxRecords = maxLoadedRecords;
	}
	
	@Override
	public RepeatStatus execute(StepContribution stepContrib, ChunkContext chunkContext)
			throws Exception {
		
		// Execution context
		ExecutionContext context = new ExecutionContext();
		
		// Check tasklet's state
		if (comparator == null)
			throw new IllegalStateException("no comparator defined");
		
		// Read first objects from each file into memory
		logger.info("opening files to merge for reading");
		Map<FlatFileItemReader<T>, T> topObjects = new HashMap<FlatFileItemReader<T>, T>();
		for (FlatFileItemReader<T> r : readers) {
			r.open(context);
			topObjects.put(r, r.read());
		}
		
		// Open output file for writing
		logger.info("opening output file");
		writer.setTransactional(false);
		writer.open(context);
		
		// Main merge loop
		logger.info("merging files . . .");
		List<T> writeList = new ArrayList<T>();
		List<FlatFileItemReader<T>> readers = new ArrayList<FlatFileItemReader<T>>();
		readers.addAll(this.readers);
		while (readers.size() > 0) {
			// Determine next object to merge into output
			int numReaders = readers.size();
			FlatFileItemReader<T> winningReader = readers.get(0);
			T winner = topObjects.get(winningReader);
			for (int i = 1; i < numReaders; i++) {
				T current = topObjects.get(readers.get(i));
				if (comparator.compare(winner, current) > 0) {
					winner = current;
					winningReader = readers.get(i);
				}
			}
			
			// Add to output
			if (writeList.size() < maxRecords) {
				writeList.add(winner);
			} else {
				writer.write(writeList);
				writeList = new ArrayList<T>();
				System.gc();
				writeList.add(winner);
			}
			
			// Update readers
			T next = winningReader.read();
			if (next == null) {
				winningReader.close();
				readers.remove(winningReader);
			} else {
				topObjects.put(winningReader, next);
			}
		}
		
		// Write remaining records to output
		if (writeList.size() > 0) {
			writer.write(writeList);
		}
		
		// Close writer
		writer.close();
		
		return RepeatStatus.FINISHED;
	}

}
