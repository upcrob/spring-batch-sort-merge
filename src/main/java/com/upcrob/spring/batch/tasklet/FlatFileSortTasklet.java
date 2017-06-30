package com.upcrob.spring.batch.tasklet;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

/**
 * Tasklet sorts the contents of the specified input file and writes the sorted
 * records into a designated output file. The sort algorithm performs an out-of
 * -core sort, allowing very large files to be processed without exceeding the
 * JVM's allocated quantity of memory, provided that the max number of rows
 * specified can be held in main memory.
 *
 * @author Rob Upcraft
 *
 * @param <T> Type of bean represented by file records.
 */
public class FlatFileSortTasklet<T> implements Tasklet {

	private FileSystemResource inputResource;
	private FileSystemResource outputResource;
	private FlatFileItemIoFactory<T> inputIoFactory;
	private FlatFileItemIoFactory<T> outputIoFactory;
	private FileSystemResource tmpDirectory;
	private Comparator<T> comparator;
	private static final int DEFAULT_MAX_ROWS_READ = 100000;
	private int maxRecords;
	private static final Log logger = LogFactory
		.getLog(FlatFileSortTasklet.class);

	public FlatFileSortTasklet() {
		inputResource = null;
		outputResource = null;
		inputIoFactory = null;
		outputIoFactory = null;
		tmpDirectory = null;
		comparator = null;
		maxRecords = DEFAULT_MAX_ROWS_READ;
	}

	/**
	 * Specifies how many records may be loaded into main memory without being
	 * written out to the destination file. If OutOfMemory errors manifest
	 * themselves, this value should be decreased.
	 *
	 * @param maxLoadedRecords Maximum number of loaded records.
	 */
	public void setMaxLoadedRecords(int maxLoadedRecords) {
		if (maxLoadedRecords < 1)
			throw new IllegalArgumentException(
				"max loaded records must be greater than 0");
		this.maxRecords = maxLoadedRecords;
	}

	@Override
	public RepeatStatus execute(StepContribution stepContrib,
		ChunkContext chunkContext) throws Exception {

		// Check tasklet's state
		if (comparator == null)
			throw new IllegalStateException("no comparator specified");
		if (inputResource == null)
			throw new IllegalStateException("no input resource specified");
		if (outputResource == null)
			throw new IllegalStateException("no output resource specified");
		if (inputIoFactory == null)
			throw new IllegalStateException("no input I/O factory defined");
		if (outputIoFactory == null)
			throw new IllegalStateException("no output I/O factory defined");
		if (this.tmpDirectory == null)
			throw new IllegalStateException(
				"no temporary working directory specified");

		// Get output and input files
		File inputFile = inputResource.getFile();
		File outputFile = outputResource.getFile();

		// Execution context
		ExecutionContext context = new ExecutionContext();

		// Prepare reader and writer
		FlatFileItemReader<T> reader = inputIoFactory.getReader(inputResource);
		reader.open(context);
		logger.info("input file, '" + inputFile.getAbsolutePath()
			+ "' opened for reading");
		FlatFileItemWriter<T> writer = inputIoFactory.getWriter(outputResource);
		writer.setTransactional(false);

		// Prepare sorting variables
		T record = null;
		List<T> records = new ArrayList<T>();
		List<FileSystemResource> tmpFiles = new ArrayList<FileSystemResource>();

		// Load first rows into memory
		logger.info("loading records into memory");
		try {
			for (int i = 0; i < maxRecords; i++) {
			record = reader.read();
			if (record == null)
				break;
			records.add(record);
			}
		} catch (Exception e) {
			reader.close();
			throw e;
		}

		if (record == null) {
			// Temporary files were not necessary, just sort in memory
			// and write to output file
			logger.info("complete input file fits in memory, no temporary files required");
			try {
				reader.close();
				if (outputFile.exists())
					outputFile.delete(); // Remove file if it already exists
				writer.open(context);
				logger.info("output file, '" + outputFile.getAbsolutePath()
					+ "' opened for writing");
				Collections.sort(records, comparator);
				writer.write(records);
				logger.info("output file written successfully");
				writer.close();
				logger.info("output file closed successfully");
			} catch (Exception e) {
				logger.error("an exception occurred while writing the output file: "
					+ e.getMessage());
				logger.info("attempting to remove '"
					+ outputFile.getAbsoluteFile()
					+ "' from the filesystem...");
				writer.close();
				if (outputFile.exists()) {
					if (outputFile.delete())
					logger.info("file deleted successfully");
					else
					logger.warn("file could not be deleted");
				}
				throw e;
			}
		} else {
			logger.info("full file doesn't fit in main memory, using out-of-core resources");
			File tmpDirectory = this.tmpDirectory.getFile();
			if (!tmpDirectory.isDirectory())
				throw new IllegalStateException("temporary space resource '"
					+ tmpDirectory.getAbsolutePath()
					+ "' is not a directory");

			// Remove existing output file
			if (outputFile.exists() && !outputFile.delete()) {
				logger.warn("couldn't remove existing output file, '"
					+ outputFile.getAbsolutePath() + "' from disk");
			}

			// Temporary files will be needed
			do {
				// Sort records currently in memory
				Collections.sort(records, comparator);

				// Prepare a temporary file and add it to the merge list
				FileSystemResource tmp = new FileSystemResource(new File(
					tmpDirectory.getAbsolutePath() + "/"
					+ outputFile.getName() + tmpFiles.size()));
				tmpFiles.add(tmp);

				// Write records to temporary file
				writeTmpFile(tmp, records, tmpFiles, context);

				// Read in next set of records
				records = new ArrayList<T>();
				for (int i = 0; i < maxRecords; i++) {
					record = reader.read();
					if (record == null)
						break;
					records.add(record);
				}
			} while (records.size() > 0);

			// Merge temporary files into output
			logger.info("merging temporary files into: '"
				+ outputFile.getAbsolutePath() + "'...");
			FlatFileMergeTasklet<T> merger = new FlatFileMergeTasklet<T>();
			merger.setWriter(writer);

			List<FlatFileItemReader<T>> readers = new ArrayList<FlatFileItemReader<T>>();
			for (FileSystemResource r : tmpFiles) {
				FlatFileItemReader<T> t = outputIoFactory.getReader(r);
				t.setResource(r);
				readers.add(t);
			}
			merger.setReaders(readers);

			merger.setComparator(comparator);
			try {
				merger.execute(null, null);
				logger.info("merge complete");
				cleanupTemporaryFiles(tmpFiles);
			} catch (Exception e) {
				logger.error("merge operation failed");
				cleanupTemporaryFiles(tmpFiles);
				throw e;
			}
		}

		return RepeatStatus.FINISHED;
	}

	/**
	 * Cleans up temporary files used during out-of-core sort.
	 *
	 * @param tmpFiles List of temporary files.
	 * @throws IOException Thrown if an I/O error occurs.
	 */
	private void cleanupTemporaryFiles(List<FileSystemResource> tmpFiles)
		throws IOException {
		logger.info("cleaning up temporary files...");
		for (Resource r : tmpFiles) {
			if (!r.getFile().delete()) {
				logger.warn("couldn't delete temporary file: '"
					+ r.getFile().getAbsolutePath()
					+ "', additional cleanup may be required");
			}
		}
		logger.info("done cleaning up temporary files");
	}

	/**
	 * Write records to a temporary file.
	 *
	 * @param tmp File resource handle.
	 * @param records Records to write. Will be written in the given order.
	 * @param tmpFiles List of current temporary files. These will be deleted if a
	 *   backout occurs.
	 * @throws Exception Thrown in the event of an I/O error.
	 */
	private void writeTmpFile(FileSystemResource tmp, List<T> records,
			List<FileSystemResource> tmpFiles, ExecutionContext context) throws Exception {

		String path = tmp.getFile().getAbsolutePath();
		logger.info("writing records to temporary file: '" + path + "'");
		if (tmp.getFile().exists()) {
			logger.warn("temporary file, '"
				+ path
				+ "' already exists, did a previous cleanup operation fail?");
		}
		FlatFileItemWriter<T> tmpWriter = outputIoFactory.getWriter(tmp);
		tmpWriter.setTransactional(false);
		tmpWriter.open(context);

		try {
			// Write records
			tmpWriter.write(records);
			tmpWriter.close();
		} catch (Exception e) {
			tmpWriter.close();
			logger.info("removing temporary files...");
			for (FileSystemResource r : tmpFiles) {
				if (!r.getFile().delete()) {
					logger.warn("failed to delete temporary file: '"
						+ r.getFile().getAbsolutePath()
						+ "', additional cleanup may be required");
				}
			}
			logger.info("done removing temporary files");
			throw e;
		}
	}

	public void setInputResource(FileSystemResource r) {
		this.inputResource = r;
	}

	public void setOutputResource(FileSystemResource r) {
		this.outputResource = r;
	}

	public void setInputIoFactory(FlatFileItemIoFactory<T> input) {
		this.inputIoFactory = input;
	}

	public void setOutputIoFactory(FlatFileItemIoFactory<T> output) {
		this.outputIoFactory = output;
	}

	public void setTmpDirectory(FileSystemResource tmpDirectory) {
		this.tmpDirectory = tmpDirectory;
	}

	public void setComparator(Comparator<T> c) {
		this.comparator = c;
	}
}
