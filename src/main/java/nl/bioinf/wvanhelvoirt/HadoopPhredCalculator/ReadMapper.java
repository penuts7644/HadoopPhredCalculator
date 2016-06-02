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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * ReadMapper
 *
 * The Mapper class will receive one RecordReader containing 1 read (4 lines), and process it.
 * The created array will be wrapped in a ArrayWritable passed on to the Reducer.
 *
 * @author Wout van Helvoirt
 */
public class ReadMapper extends Mapper<LongWritable, Text, LongWritable, ArrayWritable> {

    /** ArrayWritable to be passed on to the Reducer. */
    private ArrayWritable phredCount;

    /**
     * Override method that processes one RecordReader item and send it's output to the reducing step.
     *
     * @param key LongWritable as key.
     * @param value Text containing 1 read (4 lines) from the fastq file.
     * @param context Context containing job information.
     * @throws IOException When something went wrong.
     * @throws InterruptedException When connection was interrupted.
     */
    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // Get the configuration.
        Configuration conf = context.getConfiguration();
        AveragePhredCalculator apc = new AveragePhredCalculator(value.toString());

//        throw new IOException(value.toString());

        // Add the Writable array too the ArrayWritable wrapper and return the result.
        this.phredCount = new ArrayWritable(Text.class, apc.calculateAsciiScores());
        context.write(key, this.phredCount);
    }
}
