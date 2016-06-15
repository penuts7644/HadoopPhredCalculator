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

package nl.bioinf.wvanhelvoirt.HadoopPhredCalculator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

/**
 * FastqFileRecordWriter
 *
 * This is a custom class to write the output of the Reducer to a fastqc file.
 *
 * @author Wout van Helvoirt
 */
public class FastqFileRecordWriter extends RecordWriter<NullWritable, TextArrayWritable> {

    /** The Configuration. */
    private final Configuration mConf;
    /** Output file path. */
    private final Path mOutputPath;

    /**
     * Implementation detail: This constructor is built to be called via
     * reflection from within FileRecordWriter.
     *
     * @param context The context for this task.
     */
    public FastqFileRecordWriter(TaskAttemptContext context) {
        this.mConf = context.getConfiguration();
        this.mOutputPath = new Path(this.mConf.get("output.dir"), "PhredCalculator.fastqc");
    }

    /**
     * Override method that writes the Reducer output to a file.
     *
     * @param key   LongWritable as key, which will not be used.
     * @param value TextArrayWritable containing the count data.
     * @throws IOException          Returns default exception.
     * @throws InterruptedException If connection problem.
     */
    @Override
    public void write(NullWritable key, TextArrayWritable value)
            throws IOException, InterruptedException {

        // Set the filesystem and delete path if it exists.
        FileSystem hdfs = FileSystem.get(this.mConf);
        if (hdfs.exists(this.mOutputPath)) {
            hdfs.delete(this.mOutputPath, false);
        }

        // Write the TextArrayWritable contents to a fastqc file.
        FSDataOutputStream out = hdfs.create(this.mOutputPath);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out));
        for (Text line : value.get()) {
            bw.write(line.toString());
            bw.newLine();
        }
        bw.close();
        hdfs.close();
    }

    /**
     * Closes any connection. Not used.
     *
     * @throws IOException          Returns default exception.
     * @throws InterruptedException If connection problem.
     */
    @Override
    public void close(TaskAttemptContext context)
            throws IOException, InterruptedException {

        // no-op
    }
}
