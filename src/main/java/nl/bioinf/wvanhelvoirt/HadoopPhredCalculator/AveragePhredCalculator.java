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

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.LinkedList;

/**
 * AveragePhredCalculator
 *
 * This class can be used to calculate the average phred score values per base per read in a Fastq file.
 *
 * @author Wout van Helvoirt
 */
public class AveragePhredCalculator {

    /** Base value for correcting phred scores. */
    private int asciiBase;
    /** A string containing the read data. */
    private String[] readData;

    /**
     * Constructor sets the readData.
     *
     * @param base The base ascii value for phred correcting.
     * @param read The read data.
     */
    public AveragePhredCalculator(int base, String read) {
        this.asciiBase = base;
        this.readData = read.split("\\n");
    }

    /**
     * This function calculates the ascii values per phred score per base.
     *
     * @return IntWritable array containing ascii values per base.
     */
    public Text[] calculateAsciiScores() throws IOException {

        LinkedList<Float> sizablePhredArray = new LinkedList<>();
        LinkedList<Integer> sizableCountArray = new LinkedList<>();

        for (int i = 0; i < this.readData.length; i += 4) {

            // If the length of the base line equals the length of the phred line.
            if (this.readData[i + 1].length() == this.readData[i + 3].length()) {

                // Add the characters array to the IntWritable array.
                for (int j = 0; j < this.readData[i + 3].length(); j++) {
                    try {
                        sizablePhredArray.set(j, sizablePhredArray.get(j)
                                + ((float) this.readData[i + 3].charAt(j) - this.asciiBase));
                        sizableCountArray.set(j, sizableCountArray.get(j) + 1);
                    } catch (IndexOutOfBoundsException e) {
                        sizablePhredArray.add(j, ((float) this.readData[i + 3].charAt(j) - this.asciiBase));
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

        return phredCount;
    }
}
