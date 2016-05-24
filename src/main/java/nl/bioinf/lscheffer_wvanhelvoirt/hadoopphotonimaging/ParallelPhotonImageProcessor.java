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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.TwoDArrayWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * ParallelPhotonImageProcessor
 *
 * Test.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public final class ParallelPhotonImageProcessor extends Configured implements Tool {

    /**
     * Main function for running program.
     * @param args the command line arguments
     */
    public static void main(final String[] args) {
        try {
            int res = ToolRunner.run(new Configuration(), new ParallelPhotonImageProcessor(), args);
            System.exit(res);
        } catch (Exception e) {
            System.out.println("A problem occurred: " + e.getMessage() + "\n");
        }
    }

    /**
     * private constructor.
     */
    private ParallelPhotonImageProcessor() { }

    @Override
    public int run(String[] args) throws Exception {

        // When implementing tool
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);

        // Create job
        Job job = Job.getInstance(conf, conf.get("mapreduce.job.name", "PhotonImageProcess"));
        job.setJarByClass(ParallelPhotonImageProcessor.class);

        // Setup MapReduce job
        // Do not specify the number of Reducer
        job.setMapperClass(ImageMapper.class);
        job.setReducerClass(CountMatrixReducer.class);

        // Specify key / value
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(TwoDArrayWritable.class);

        if (conf.get("input.dir") != null && conf.get("output.dir") != null) {
            // Input
            WholeFileInputFormat.setInputPathFilter(job, TiffPathFilter.class);
            WholeFileInputFormat.setInputPaths(job, new Path(conf.get("input.dir")));
            job.setInputFormatClass(WholeFileInputFormat.class);

            // Output
            Path output = new Path(conf.get("output.dir"));
            fs.delete(output, true);
            FileOutputFormat.setOutputPath(job, output);
            job.setOutputFormatClass(TextOutputFormat.class);
        } else {
            throw new IllegalArgumentException("The value of property input.dir and output.dir must not be null");
        }

        // Execute job and return status.
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
