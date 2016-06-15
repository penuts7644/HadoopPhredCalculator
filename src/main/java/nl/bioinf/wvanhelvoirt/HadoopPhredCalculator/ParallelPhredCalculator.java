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
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * ParallelPhredCalculator
 *
 * This class runs the Hadoop MapReduce job. It assigns a mapper and reducer and is able to calculate the average phred
 * score per base per read in a FastQ file. Users can change the job name by assigning a value to the
 * 'mapreduce.job.name' option. 'input.files' and 'output.dir' options are required.
 *
 * @author Wout van Helvoirt
 */
public final class ParallelPhredCalculator extends Configured implements Tool {

    /**
     * Private constructor, necessary for the ToolRunner in main.
     */
    private ParallelPhredCalculator() {
    }

    /**
     * Main function for running the program.
     *
     * @param args the command line arguments.
     */
    public static void main(final String[] args) {

        // Try to make a ToolRunner, so hadoop specific command-line arguments will be parsed.
        try {
            int res = ToolRunner.run(new Configuration(), new ParallelPhredCalculator(), args);
            System.exit(res);
        } catch (Exception e) {
            System.out.println("A problem occurred: " + e.getMessage());
        }
    }

    /**
     * ToolRunner override method which contains the code to run the Hadoop MapReduce job.
     *
     * @param args Command-line arguments.
     * @return int if the job is done.
     * @throws Exception will be caught in the main.
     */
    @Override
    public int run(String[] args) throws Exception {

        // Set filesystem and get the configuration given by ToolRunner.
        Configuration conf = this.getConf();
        FileSystem hdfs = FileSystem.get(conf);

        // Create job with configuration, name and set the main class for the jar file.
        Job job = Job.getInstance(conf, conf.get("mapreduce.job.name", "PhredCalculator"));
        job.setJarByClass(ParallelPhredCalculator.class);

        // Set the mapper and reducer classes.
        job.setMapperClass(ReadMapper.class);
        job.setReducerClass(CombineReducer.class);

        // Specify the mapper output key and value classes.
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(TextArrayWritable.class);

        // If 'input.dir' and/or 'output.dir' not given, throw exception.
        if (conf.get("input.files") != null && conf.get("output.dir") != null) {

            // Set a input path filter to use only fastq/fq files in directory and set input formatting class.
            NReadInputFormat.setNumLinesPerSplit(job, (4 * conf.getInt("reads.per.map", 2000)));
            NReadInputFormat.setInputPathFilter(job, FastqPathFilter.class);
            NReadInputFormat.setInputPaths(job, new Path(conf.get("input.files")));
            job.setInputFormatClass(NReadInputFormat.class);

            // Delete output path on filesystem if exists and set output formatting class.
            Path output = new Path(conf.get("output.dir"));
            if (hdfs.exists(output)) {
                hdfs.delete(output, true);
            }
            FastqFileOutputFormat.setOutputPath(job, output);
            job.setOutputFormatClass(FastqFileOutputFormat.class);
        } else {
            throw new IllegalArgumentException("The value of property input.files and output.dir must not be null");
        }

        // Execute job and return status.
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
