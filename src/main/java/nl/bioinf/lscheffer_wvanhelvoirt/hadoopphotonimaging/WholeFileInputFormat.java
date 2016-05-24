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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * WholeFileInputFormat
 *
 * This is a custom class for the tiff input, so files won't be split.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    /**
     * Creates a CombineFileRecordReader to read each file assigned to this InputSplit.
     * Note, that unlike ordinary InputSplits, split must be a CombineFileSplit, and therefore
     * is expected to specify multiple files.
     *
     * @param split The InputSplit to read. Throws an IllegalArgumentException if this is not a CombineFileSplit.
     * @param context The context for this task.
     * @return a CombineFileRecordReader to process each file in split. It will read each file with a WholeFileRecordReader.
     * @throws IOException if there is an error.
     */
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        if (!(split instanceof FileSplit)) {
            throw new IllegalArgumentException("Split must be a FileSplit");
        }
        return new WholeFileRecordReader((FileSplit) split, context);
        //return new CombineFileRecordReader<NullWritable, BytesWritable>((CombineFileSplit) split, context, WholeFileRecordReader.class);
    }
}
