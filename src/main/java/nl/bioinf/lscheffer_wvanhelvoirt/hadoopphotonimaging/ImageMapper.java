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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * ImageMapper
 *
 * Test.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class ImageMapper extends Mapper<NullWritable, BytesWritable, NullWritable, TwoDArrayWritable> {

    private TwoDArrayWritable finalPhotonCountMatrix = new TwoDArrayWritable(IntWritable.class);
    private IntWritable[][] photonCountMatrix;

    public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        ByteArrayInputStream bais = new ByteArrayInputStream(value.getBytes());
        PhotonImageProcessor pip = new PhotonImageProcessor(bais, conf.getInt("tolerance", 100), conf.get("method", "Fast"), conf.getBoolean("preprocessing", true));
        pip.run();

        int[][] matrix = pip.getPhotonCountMatrix();
        this.photonCountMatrix = new IntWritable[matrix[0].length][matrix.length];
        for (int i = 0; i < matrix[0].length; i++) {
            for (int j = 0; j < matrix.length; j++) {
                this.photonCountMatrix[i][j] = new IntWritable(matrix[i][j]);
            }
        }
        this.finalPhotonCountMatrix.set(this.photonCountMatrix);
        context.write(NullWritable.get(), this.finalPhotonCountMatrix);
    }
}
