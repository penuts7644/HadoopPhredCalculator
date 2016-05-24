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
 * WholeFileRecordReader
 *
 * This is a custom class for the tiff input, so files won't be split.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

    /** The path to the file to read. */
    private final Path mFileToRead;
    /** The length of this file. */
    private final long mFileLength;
    /** The Configuration. */
    private final Configuration mConf;
    /** Whether this FileSplit has been processed. */
    private boolean mProcessed;
    /** Single Text to store the value of this file (the value) when it is read. */
    private final BytesWritable mFileBytes;

    /**
     * Implementation detail: This constructor is built to be called via
     * reflection from within CombineFileRecordReader.
     *
     * @param fileSplit The CombineFileSplit that this will read from.
     * @param context The context for this task.
     */
    public WholeFileRecordReader(FileSplit fileSplit, TaskAttemptContext context) {
        this.mProcessed = false;
        this.mFileToRead = fileSplit.getPath();
        this.mFileLength = fileSplit.getLength();
        this.mConf = context.getConfiguration();
        this.mFileBytes = new BytesWritable();
    }

    /** {@inheritDoc} */
    @Override
    public void close() throws IOException {
        // no-op
    }

    /**
     * Returns the absolute path to the current file.
     *
     * @return The absolute path to the current file.
     * @throws IOException never.
     * @throws InterruptedException never.
     */
    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    /**
     * Returns the current value. If the file has been read with a call to NextKeyValue(),
     * this returns the contents of the file as a BytesWritable.  Otherwise, it returns an
     * empty BytesWritable.
     *
     * Throws an IllegalStateException if initialize() is not called first.
     *
     * @return A BytesWritable containing the contents of the file to read.
     * @throws IOException never.
     * @throws InterruptedException never.
     */
    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return this.mFileBytes;
    }

    /**
     * Returns whether the file has been processed or not. Since only one record
     * will be generated for a file, progress will be 0.0 if it has not been processed,
     * and 1.0 if it has.
     *
     * @return 0.0 if the file has not been processed. 1.0 if it has.
     * @throws IOException never.
     * @throws InterruptedException never.
     */
    @Override
    public float getProgress() throws IOException, InterruptedException {
        return (this.mProcessed) ? (float) 1.0 : (float) 0.0;
    }

    /**
     * All of the internal state is already set on instantiation. This is a no-op.
     *
     * @param split The InputSplit to read.  Unused.
     * @param context The context for this task.  Unused.
     * @throws IOException never.
     * @throws InterruptedException never.
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        // no-op.
    }

    /**
     * If the file has not already been read, this reads it into memory, so that a call
     * to getCurrentValue() will return the entire contents of this file as Text,
     * and getCurrentKey() will return the qualified path to this file as Text. Then, returns
     * true. If it has already been read, then returns false without updating any internal state.
     *
     * @return Whether the file was read or not.
     * @throws IOException if there is an error reading the file.
     * @throws InterruptedException if there is an error.
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!this.mProcessed) {
            if (this.mFileLength > (long) Integer.MAX_VALUE) {
                throw new IOException("File is longer than Integer.MAX_VALUE.");
            }
            byte[] contents = new byte[(int) this.mFileLength];

            FileSystem fs = this.mFileToRead.getFileSystem(this.mConf);
            InputStream in = null;
            try {
                // Set the contents of this file.
                in = fs.open(this.mFileToRead);
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