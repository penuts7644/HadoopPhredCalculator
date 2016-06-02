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

import org.apache.hadoop.io.IntWritable;

/**
 * AveragePhredCalculator
 *
 * This class can be used to calculate the average Phred score values per base per read in a FastQ file.
 *
 * @author Wout van Helvoirt
 */
public class AveragePhredCalculator {

    /** A string containing the read data. */
    private String[] readData;

    /**
     * Constructor sets the readData.
     *
     * @param read The read data.
     */
    public AveragePhredCalculator(String read) {
        this.readData = read.split("\\n");
    }

    /**
     * This function calculates the ASCII values per phred score per base.
     *
     * @return IntWritable array containing ascii values per base.
     */
    public IntWritable[] calculateAsciiScores() {

        // If the length of the base line equals the length of the phred line.
        if (this.readData[1].length() == this.readData[3].length()) {

            // Convert phred's to characters.
            char[] ordValues = new char[this.readData[3].length()];
            this.readData[3].getChars(0, this.readData[3].length(), ordValues, 0);

            // Add the characters array to the Text array.
            IntWritable[] phredCountArray = new IntWritable[ordValues.length];
            for (int i = 0; i < phredCountArray.length; i++) {
                phredCountArray[i] = new IntWritable((int) ordValues[i]);
            }

            return phredCountArray;
        } else {
            return new IntWritable[0];
        }
    }
}
