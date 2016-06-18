/*
 * Copyright (c) 2016 Wout van Helvoirt
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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.LinkedList;

/**
 * ReadMapper
 *
 * The Mapper class will receive one RecordReader containing reads (one read is 4 lines), and process it.
 * The created array will be wrapped in a TextArrayWritable passed on to the Reducer.
 *
 * @author Wout van Helvoirt
 */
public class ReadMapper extends Mapper<LongWritable, Text, NullWritable, TextArrayWritable> {

    /**
     * Override method that processes one RecordReader item and send it's output to the reducing step.
     *
     * @param key     LongWritable as key.
     * @param value   Text containing reads (one read is 4 lines) from the fastq file.
     * @param context Context containing job information.
     * @throws IOException          When something went wrong.
     * @throws InterruptedException When connection was interrupted.
     */
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // Set the configuration, read data and ascii base value.
        Configuration conf = context.getConfiguration();
        int asciiBase = conf.getInt("ascii.base", 64);
        String[] readData = value.toString().split("\\n");

        LinkedList<Float> sizablePhredArray = new LinkedList<>();
        LinkedList<Integer> sizableCountArray = new LinkedList<>();

        for (int i = 0; i < readData.length; i += 4) {

            // If the length of the base line equals the length of the phred line.
            if (readData[i + 1].length() == readData[i + 3].length()) {

                // Add the characters array to the IntWritable array.
                for (int j = 0; j < readData[i + 3].length(); j++) {
                    try {
                        sizablePhredArray.set(j, sizablePhredArray.get(j)
                                + ((float) readData[i + 3].charAt(j) - asciiBase));
                        sizableCountArray.set(j, sizableCountArray.get(j) + 1);
                    } catch (IndexOutOfBoundsException e) {
                        sizablePhredArray.add(j, ((float) readData[i + 3].charAt(j) - asciiBase));
                        sizableCountArray.add(j, 1);
                    }
                }
            }
        }

        // Instantiate the Text array and add lines.
        Text[] phredCount = new Text[sizablePhredArray.size()];
        for (int i = 0; i < sizablePhredArray.size(); i++) {
            phredCount[i] = new Text(sizablePhredArray.get(i) + "|" + sizableCountArray.get(i));
        }

        // Add the IntWritable array too the TextArrayWritable wrapper and return the result.
        context.write(NullWritable.get(), new TextArrayWritable(Text.class, phredCount));
    }
}
