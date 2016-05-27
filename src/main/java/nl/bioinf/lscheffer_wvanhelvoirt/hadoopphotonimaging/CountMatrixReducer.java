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

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * CountMatrixReducer
 *
 * Test.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class CountMatrixReducer extends Reducer<NullWritable, IntTwoDArrayWritable, NullWritable, IntTwoDArrayWritable> {

    private IntTwoDArrayWritable finalPhotonCountMatrix;
    private IntWritable[][] photonCountMatrix;

    public void reduce(NullWritable key, Iterable<IntTwoDArrayWritable> values, Context context) throws IOException, InterruptedException {

        for (IntTwoDArrayWritable value : values) {
            IntWritable[][] matrix = value.get();
            if (this.photonCountMatrix == null) {
                this.photonCountMatrix = new IntWritable[matrix[0].length][matrix.length];
                for (int i = 0; i < matrix[0].length; i++){
                    for (int j = 0; j < matrix.length; j++) {
                        this.photonCountMatrix[i][j] = new IntWritable(matrix[i][j].get());
                    }
                }
            } else {
                for (int i = 0; i < matrix[0].length; i++){
                    for (int j = 0; j < matrix.length; j++) {
                        this.photonCountMatrix[i][j].set(this.photonCountMatrix[i][j].get() + matrix[i][j].get());
                    }
                }
            }
        }
        this.finalPhotonCountMatrix = new IntTwoDArrayWritable(IntWritable.class, this.photonCountMatrix);
        context.write(NullWritable.get(), this.finalPhotonCountMatrix);
    }
}