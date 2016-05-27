/*
 * Copyright (c) 2016 Lonneke Scheffer and Wout van Helvoirt
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package nl.bioinf.lscheffer_wvanhelvoirt.hadoopphotonimaging;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.io.IOException;
import java.io.InputStream;

/**
 * ImageFileRecordReader
 *
 * This is a custom class to create a RecordReader for each split.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class ImageFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

    /** The path to the file to read. */
    private final Path mFileToRead;
    /** The length of this file. */
    private final long mFileLength;
    /** The Configuration. */
    private final Configuration mConf;
    /** Whether this FileSplit has been processed. */
    private boolean mProcessed;
    /** A BytesWritable to store the byte array. */
    private final BytesWritable mFileBytes;

    /**
     * Implementation detail: This constructor is built to be called via
     * reflection from within FileRecordReader.
     *
     * @param fileSplit The FileSplit that this will read from.
     * @param context The context for this task.
     */
    public ImageFileRecordReader(FileSplit fileSplit, TaskAttemptContext context) {
        this.mProcessed = false;
        this.mFileToRead = fileSplit.getPath();
        this.mFileLength = fileSplit.getLength();
        this.mConf = context.getConfiguration();
        this.mFileBytes = new BytesWritable();
    }

    /**
     * Closes any connection. Not used.
     *
     * @throws IOException Returns default exception.
     */
    @Override
    public void close() throws IOException {
        // Not used.
    }

    /**
     * Override method that returns a NullWritable as key, because key is not being used.
     *
     * @return NullWritable because key will not be used in program.
     * @throws IOException Returns default exception.
     * @throws InterruptedException Returns default exception.
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * Override method that returns the current value. If the file has been read with a call to NextKeyValue(),
     * this returns the contents of the file as a BytesWritable. Otherwise, it returns an empty BytesWritable.
     *
     * @return BytesWritable containing the contents of the file.
     * @throws IOException Returns default exception.
     * @throws InterruptedException Returns default exception.
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return this.mFileBytes;
    }

    /**
     * Override method that returns whether the file has been processed or not. Can be 0.0 or 1.0 because file will
     * not be split.
     *
     * @return Float 0.0 if the file has not been processed. 1.0 if it has.
     * @throws IOException Returns default exception.
     * @throws InterruptedException Returns default exception.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (this.mProcessed) ? (float) 1.0 : (float) 0.0;
    }

    /**
     * Override method for instantiation. Not used.
     *
     * @param split The InputSplit to read.
     * @param context The context for this task.
     * @throws IOException Returns default exception.
     * @throws InterruptedException Returns default exception.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // Not used.
    }

    /**
     * Override method that if the file has not already been read, reads it into memory, so that a call
     * to getCurrentValue() will return the entire contents of this file as BytesWritable. Then, returns
     * true. If it has already been read, then returns false without updating any internal state.
     *
     * @return Boolean whether the file was read or not.
     * @throws IOException If there is an error reading the file.
     * @throws InterruptedException If there is an error.
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {

        // If file not processed, process it.
        if (!this.mProcessed) {
            if (this.mFileLength > (long) Integer.MAX_VALUE) {
                throw new IOException("file is longer than Integer.MAX_VALUE.");
            }
            byte[] contents = new byte[(int) this.mFileLength];

            // Read file from hdfs to byte array and set the BytesWritable.
            FileSystem hdfs = this.mFileToRead.getFileSystem(this.mConf);
            InputStream in = null;
            try {
                // Set the contents of this file.
                in = hdfs.open(this.mFileToRead);
                IOUtils.readFully(in, contents, 0, contents.length);
                this.mFileBytes.set(contents, 0, contents.length);

            } finally {
                IOUtils.closeStream(in);
            }
            this.mProcessed = true;
            return true;
        }
        return false;
    }
}