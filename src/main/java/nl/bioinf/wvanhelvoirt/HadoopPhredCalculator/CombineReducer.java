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

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;

/**
 * CombineReducer
 * The Reducer class that combines the data from all the mappers to a single array.
 *
 * @author Wout van Helvoirt
 */
public class CombineReducer extends Reducer<NullWritable, TextArrayWritable, NullWritable, TextArrayWritable> {

    /**
     * Final TextArrayWritable with combined counts.
     */
    private TextArrayWritable finalPhredCount;
    /**
     * The text array containing the combined counts.
     */
    private Text[] phredCount;

    /**
     * Override method that processes all mapper outputs to one array, ready to be written as file.
     *
     * @param key     NullWritable not used.
     * @param values  Iterable with AveragePhredCalculator items from each mapper.
     * @param context Context containing job information.
     * @throws IOException          When something went wrong.
     * @throws InterruptedException When connection was interrupted.
     */
    @Override
    public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context)
            throws IOException, InterruptedException {

        LinkedList<Float> sizablePhredArray = new LinkedList<>();
        LinkedList<Integer> sizableCountArray = new LinkedList<>();

        // For each Mapper output, add the values to photonCountMatrix.
        for (TextArrayWritable value : values) {
            Text[] asciiArray = value.get();

            // IntWritable arrays containing values, add these to the sizableArray only if not empty.
            if (asciiArray.length > 0) {
                for (int i = 0; i < asciiArray.length; i++) {
                    try {
                        sizablePhredArray.set(i, sizablePhredArray.get(i)
                                + Float.parseFloat(asciiArray[i].toString().split("\\|")[0]));
                        sizableCountArray.set(i, sizableCountArray.get(i)
                                + Integer.parseInt(asciiArray[i].toString().split("\\|")[1]));
                    } catch (IndexOutOfBoundsException e) {
                        sizablePhredArray.add(i, Float.parseFloat(asciiArray[i].toString().split("\\|")[0]));
                        sizableCountArray.add(i, Integer.parseInt(asciiArray[i].toString().split("\\|")[1]));
                    }
                }
            }
        }

        // Instantiate the Text array and add lines.
        this.phredCount = new Text[sizablePhredArray.size()];
        this.phredCount[0] = new Text("base_position\taverage_phred_score");
        for (int i = 1; i < sizablePhredArray.size(); i++) {
            this.phredCount[i] = new Text(i + "\t" + (sizablePhredArray.get(i - 1) / sizableCountArray.get(i - 1)));
        }

        // Add the Text array to the ArrayWritable wrapper and return the result.
        this.finalPhredCount = new TextArrayWritable(Text.class, this.phredCount);
        context.write(NullWritable.get(), this.finalPhredCount);
    }
}