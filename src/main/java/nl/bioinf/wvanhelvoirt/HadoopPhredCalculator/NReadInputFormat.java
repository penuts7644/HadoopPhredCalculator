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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;

import java.io.IOException;

/**
 * NReadInputFormat
 *
 * This is a custom InputFormat class for Multiple readlines per mapper.
 *
 * @author Wout van Helvoirt
 */
public class NReadInputFormat extends NLineInputFormat {

    /**
     * Creates a NReadRecordReader to read each file assigned to this InputSplit.
     *
     * @param split   The InputSplit to read. Throws an IllegalArgumentException if this is not a FileSplit.
     * @param context The context for this task.
     * @return NReadRecordReader to process each file in split.
     * @throws IOException If there is an error.
     */
    @Override
    public RecordReader<LongWritable, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {

        // Return the new NReadRecordReader.
        return new NReadRecordReader();
    }
}
