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

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * ParallelPhotonImageProcessor
 *
 * Test.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public final class ParallelPhotonImageProcessor {

    /**
     * Main function for running program.
     * @param args the command line arguments
     */
    public static void main(final String[] args) {
        ParallelPhotonImageProcessor mainObject = new ParallelPhotonImageProcessor();
        mainObject.start(args);
    }

    /**
     * private constructor.
     */
    private ParallelPhotonImageProcessor() { }

    /**
     * starts the application.
     * @param args the command line arguments passed from main()
     */
    private void start(final String[] args) {

        /* Initialize argument options and retrieve input from user. */
        ArgumentParser arguments = new ArgumentParser(args);
        List parsedArguments = arguments.parseArguments();
        TiffPathFilter tpf = new TiffPathFilter(".*\\.[Tt]+?[Ii]+?[Ff]+?[Ff]?");

        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            Job job = new Job(conf, parsedArguments.get(0).toString());
            job.setJarByClass(ParallelPhotonImageProcessor.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setInputFormatClass(WholeFileInputFormat.class);

            job.setMapperClass(ImageMapper.class);
            job.setReducerClass(CountMatrixReducer.class);

            FileInputFormat.setInputPathFilter(job, TiffPathFilter.class);

            FileInputFormat.setInputPaths(job, new Path(parsedArguments.get(1).toString()));
            Path output=new Path(parsedArguments.get(2).toString());
            try {
                fs.delete(output, true);
            } catch (IOException e) {
                System.out.println("A problem occured: " + e + "\n");
            }
            FileOutputFormat.setOutputPath(job, output);

            boolean wfc = job.waitForCompletion(true);
            if(!wfc){
                System.out.println("A problem occured: Job failed\n");
            } else {
                System.exit(wfc ? 0 : 1);
            }
        } catch (Exception e) {
            System.out.println("A problem occured: " + e + "\n");
        }
    }
}
