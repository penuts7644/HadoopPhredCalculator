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

package nl.bioinf.lscheffer_wvanhelvoirt.HadoopPhotonImaging;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * ImageFileOutputFormat
 *
 * This is a custom OutputFormat class for image file .
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class ImageFileOutputFormat extends FileOutputFormat<NullWritable, IntTwoDArrayWritable> {

    /**
     * Creates a ImageFileRecordWriter to write the output from the Reducer to a file.
     *
     * @param context The context for this task.
     * @return ImageFileRecordReader to process the output from the Reducer.
     * @throws IOException If there is an error.
     */
    @Override
    public RecordWriter<NullWritable, IntTwoDArrayWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {

        // Return the new ImageFileRecordWriter.
        return new ImageFileRecordWriter(context);
    }
}
