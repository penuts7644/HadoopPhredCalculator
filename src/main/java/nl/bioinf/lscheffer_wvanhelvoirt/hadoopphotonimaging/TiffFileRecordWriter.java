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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.IOException;

/**
 * TiffFileRecordWriter
 *
 * This is a custom class for the tiff input, so files won't be split.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class TiffFileRecordWriter extends RecordWriter<NullWritable, IntTwoDArrayWritable> {

    /** The Configuration. */
    private final Configuration mConf;
    /** Output file path. */
    private final Path mOutputPath;

    /**
     * Implementation detail: This constructor is built to be called via
     * reflection from within FileRecordWriter.
     *
     * @param context The context for this task.
     */
    public TiffFileRecordWriter(TaskAttemptContext context) {
        this.mConf = context.getConfiguration();
        this.mOutputPath = new Path(this.mConf.get("output.dir"), "PhotonImageProcessed.png");
    }

    @Override
    public void write(NullWritable key, IntTwoDArrayWritable value) throws IOException, InterruptedException {
        FileSystem hdfs = FileSystem.get(this.mConf);
        // Set the contents of this file.
        if (hdfs.exists(this.mOutputPath)) {
            hdfs.delete(this.mOutputPath, false);
        }
        BufferedImage bi = new PhotonImageProcessor().createOutputBufferedImage(value.get());
        ImageIO.write(bi, "png", hdfs.create(this.mOutputPath));
        hdfs.close();
    }

    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        // no-op
    }
}
