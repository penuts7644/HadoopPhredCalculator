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
 * ImageFileInputFormat
 *
 * This is a custom InputFormat class for image file that should not be split.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class ImageFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

    /**
     * Method that tells if a given image file is splittable.
     *
     * @param context The job context.
     * @param file The path of the file.
     * @return boolean if the file should be split.
     */
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    /**
     * Creates a ImageFileRecordReader to read each file assigned to this InputSplit.
     *
     * @param split The InputSplit to read. Throws an IllegalArgumentException if this is not a FileSplit.
     * @param context The context for this task.
     * @return ImageFileRecordReader to process each file in split.
     * @throws IOException If there is an error.
     */
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        if (!(split instanceof FileSplit)) {
            throw new IllegalArgumentException("split must be a FileSplit");
        }

        // Return the new ImageFileRecordReader.
        return new ImageFileRecordReader((FileSplit) split, context);
    }
}
