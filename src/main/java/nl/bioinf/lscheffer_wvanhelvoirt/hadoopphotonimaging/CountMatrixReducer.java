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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * CountMatrixReducer
 *
 * The Reducer class that combines the data from all the mappers to a single two D array.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class CountMatrixReducer extends Reducer<NullWritable, IntTwoDArrayWritable, NullWritable, IntTwoDArrayWritable> {

    /** Final IntTwoDArrayWritable with combined counts. */
    private IntTwoDArrayWritable finalPhotonCountMatrix;
    /** A Intermediate two D array. */
    private IntWritable[][] photonCountMatrix;

    /**
     * Override method that processes all mapper outputs to one two D array, ready to be written as file.
     *
     * @param key NullWritable will not be used.
     * @param values Iterable with IntTwoDArrayWritable items from each mapper.
     * @param context Context containing job information.
     * @throws IOException When something went wrong.
     * @throws InterruptedException When connection was interrupted.
     */
    @Override
    public void reduce(NullWritable key, Iterable<IntTwoDArrayWritable> values, Context context) throws IOException, InterruptedException {

        // For each Mapper output, add the values to photonCountMatrix.
        for (IntTwoDArrayWritable value : values) {
            IntWritable[][] matrix = value.get();

            // The first image should initialize the photonCountMatrix with it's values.
            if (this.photonCountMatrix == null) {
                this.photonCountMatrix = new IntWritable[matrix[0].length][matrix.length];
                for (int i = 0; i < matrix[0].length; i++){
                    for (int j = 0; j < matrix.length; j++) {
                        this.photonCountMatrix[i][j] = new IntWritable(matrix[i][j].get());
                    }
                }

            // Other images will add there values to the photonCountMatrix.
            } else {
                for (int i = 0; i < matrix[0].length; i++){
                    for (int j = 0; j < matrix.length; j++) {
                        this.photonCountMatrix[i][j].set(this.photonCountMatrix[i][j].get() + matrix[i][j].get());
                    }
                }
            }
        }

        // Add the IntWritable two D array to the IntTwoDArrayWritable wrapper and return the result.
        this.finalPhotonCountMatrix = new IntTwoDArrayWritable(IntWritable.class, this.photonCountMatrix);
        context.write(NullWritable.get(), this.finalPhotonCountMatrix);
    }
}