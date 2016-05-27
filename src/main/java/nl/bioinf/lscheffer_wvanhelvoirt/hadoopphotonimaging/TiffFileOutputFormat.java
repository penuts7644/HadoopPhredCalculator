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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * TiffFileOutputFormat
 *
 * Test.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class TiffFileOutputFormat extends FileOutputFormat<NullWritable, IntTwoDArrayWritable> {

    /**
     * Creates a FileRecordWriter to read each file assigned to this InputSplit.
     * Note, that unlike ordinary InputSplits, split must be a FileSplit, and therefore
     * is expected to specify multiple files.
     *
     * @param context The context for this task.
     * @return a CombineFileRecordReader to process each file in split. It will read each file with a ImageFileRecordReader.
     * @throws IOException if there is an error.
     */
    @Override
    public RecordWriter<NullWritable, IntTwoDArrayWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        return new TiffFileRecordWriter(context);
    }
}
