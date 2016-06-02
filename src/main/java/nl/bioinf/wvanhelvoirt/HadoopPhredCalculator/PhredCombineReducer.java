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

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.ArrayList;

/**
 * PhredCombineReducer
 *
 * The Reducer class that combines the data from all the mappers to a single array.
 *
 * @author Wout van Helvoirt
 */
public class PhredCombineReducer extends Reducer<LongWritable, ArrayWritable, LongWritable, ArrayWritable> {

    /** Final ArrayWritable with combined counts. */
    private ArrayWritable finalPhredCount;
    /** The text array containing the combined counts. */
    private Text[] phredCount;

    /**
     * Override method that processes all mapper outputs to one two D array, ready to be written as file.
     *
     * @param key NullWritable will not be used.
     * @param values Iterable with AveragePhredCalculator items from each mapper.
     * @param context Context containing job information.
     * @throws IOException When something went wrong.
     * @throws InterruptedException When connection was interrupted.
     */
    @Override
    public void reduce(LongWritable key, Iterable<ArrayWritable> values, Context context) throws IOException, InterruptedException {

        ArrayList<Integer> sizablePhredArray = new ArrayList<>();
        ArrayList<Integer> sizableCountArray = new ArrayList<>();

        // For each Mapper output, add the values to photonCountMatrix.
        for (ArrayWritable value : values) {
            IntWritable[] asciiArray  = (IntWritable[]) value.get();

            // IntWritable arrays containing values, add these to the sizableArray.
            if (asciiArray.length > 0) {
                for (int i = 0; i < asciiArray.length; i++){
                    try {
                        sizablePhredArray.set(i, sizablePhredArray.get(i) + asciiArray[i].get());
                        sizableCountArray.set(i, sizableCountArray.get(i) + 1);
                    } catch (IndexOutOfBoundsException e) {
                        sizablePhredArray.add(i, asciiArray[i].get());
                        sizableCountArray.add(i, 1);
                    }
                }

            // Continue if empty IntWritable array received.
            } else {
                continue;
            }
        }

        // Instantiate the Text array and add lines.
        this.phredCount = new Text[sizablePhredArray.size()];
        this.phredCount[0] = new Text("base_nr,sum_PHRED");
        for (int i = 1; i <sizablePhredArray.size(); i++) {
            this.phredCount[i] = new Text(i + "," + (sizablePhredArray.get(i-1) / sizableCountArray.get(i-1)));
        }

        // Add the Text array to the ArrayWritable wrapper and return the result.
        this.finalPhredCount = new ArrayWritable(Text.class, this.phredCount);
        context.write(key, this.finalPhredCount);
    }
}