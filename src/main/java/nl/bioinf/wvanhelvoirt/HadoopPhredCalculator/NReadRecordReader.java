/*
 * Copyright (c) 2016 Wout van Helvoirt
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

package nl.bioinf.wvanhelvoirt.HadoopPhredCalculator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

/**
 * NReadRecordReader
 *
 * This is a custom class to create a RecordReader for each split.
 *
 * @author Wout van Helvoirt
 */
public class NReadRecordReader extends RecordReader<LongWritable, Text> {

    /** After which lines to stop. */
    private int NLINESTOPROCESS;
    /** The lineReader. */
    private LineReader in;
    /** The LongWritable key. */
    private LongWritable key;
    /** The Text containing lines. */
    private Text value;
    /** Start position. */
    private long start;
    /** End position. */
    private long end;
    /** Current position. */
    private long pos;
    /** Max line length. */
    private int maxLineLength;

    /**
     * Closes any connection.
     *
     * @throws IOException Returns default exception.
     */
    @Override
    public void close()
            throws IOException {

        if (this.in != null) {
            this.in.close();
        }
    }

    /**
     * Override method that returns a LongWritable as key.
     *
     * @return LongWritable key.
     * @throws IOException          Returns default exception.
     * @throws InterruptedException Returns default exception.
     */
    @Override
    public LongWritable getCurrentKey()
            throws IOException, InterruptedException {

        return this.key;
    }

    /**
     * Override method that returns the current value. If the file has been read with a call to NextKeyValue(),
     * this returns the contents of the file as a Text.
     *
     * @return Text containing the contents of the file.
     * @throws IOException          Returns default exception.
     * @throws InterruptedException Returns default exception.
     */
    @Override
    public Text getCurrentValue()
            throws IOException, InterruptedException {

        return this.value;
    }

    /**
     * Override method that returns whether the file has been processed or not.
     *
     * @return Float 0.0 if the file has not been processed. 1.0 if it has.
     * @throws IOException          Returns default exception.
     * @throws InterruptedException Returns default exception.
     */
    @Override
    public float getProgress()
            throws IOException, InterruptedException {

        // Return progress state.
        if (this.start == this.end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (this.pos - this.start) / (float) (this.end - this.start));
        }
    }

    /**
     * Override method for instantiation.
     *
     * @param inputSplit The InputSplit to read.
     * @param context    The context for this task.
     * @throws IOException          Returns default exception.
     * @throws InterruptedException Returns default exception.
     */
    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {

        // Initialize.
        Configuration conf = context.getConfiguration();
        FileSplit split = (FileSplit) inputSplit;
        Path file = split.getPath();
        FileSystem fs = file.getFileSystem(conf);
        FSDataInputStream infile = fs.open(split.getPath());

        // Use number of lines given by user and set parameters.
        this.NLINESTOPROCESS = NLineInputFormat.getNumLinesPerSplit(context);
        this.maxLineLength = conf.getInt("mapreduce.input.linerecordreader.line.maxlength", Integer.MAX_VALUE);
        this.start = split.getStart();
        this.end = this.start + split.getLength();
        boolean skipFirstLine = false;

        // Skip first line?
        if (this.start != 0) {
            skipFirstLine = true;
            this.start--;
            infile.seek(this.start);
        }
        this.in = new LineReader(infile, conf);
        if (skipFirstLine) {
            this.start += this.in.readLine(new Text(), 0,
                    (int) Math.min((long) Integer.MAX_VALUE, this.end - this.start));
        }
        this.pos = this.start;
    }

    /**
     * Override method that if the file has not already been read, reads it into memory, so that a call to
     * getCurrentValue() will return the lines this file as Text. Then, returns true. If it has already been read,
     * then returns false without updating any internal state.
     *
     * @return Boolean whether the file was read or not.
     * @throws IOException          If there is an error reading the file.
     * @throws InterruptedException If there is an error.
     */
    @Override
    public boolean nextKeyValue()
            throws IOException, InterruptedException {

        // Initialize key and value.
        if (this.key == null) {
            this.key = new LongWritable();
        }
        if (this.value == null) {
            this.value = new Text();
        }

        // Get the key and value.
        this.key.set(this.pos);
        this.value.clear();
        Text endline = new Text("\n");
        int newSize = 0;
        for (int i = 0; i < this.NLINESTOPROCESS; i++) {
            Text v = new Text();
            while (this.pos < this.end) {
                newSize = this.in.readLine(v, this.maxLineLength,
                        Math.max((int) Math.min(Integer.MAX_VALUE, this.end - this.pos), this.maxLineLength));
                this.value.append(v.getBytes(), 0, v.getLength());
                this.value.append(endline.getBytes(), 0, endline.getLength());
                if (newSize == 0) {
                    break;
                }
                this.pos += newSize;
                if (newSize < this.maxLineLength) {
                    break;
                }
            }
        }

        // If newSize is still zero, return false, else true.
        if (newSize == 0) {
            this.key = null;
            this.value = null;
            return false;
        } else {
            return true;
        }
    }
}