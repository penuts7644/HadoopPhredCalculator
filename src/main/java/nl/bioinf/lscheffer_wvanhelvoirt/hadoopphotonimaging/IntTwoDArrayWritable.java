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
import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * IntTwoDArrayWritable
 *
 * A Custom Writable class based on the TwoDArrayWritable, but uses IntWritable's instead of Writable's.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class IntTwoDArrayWritable implements Writable {

    /** The class type. */
    private Class valueClass;
    /** A IntWritable two D array to be used internally. */
    private IntWritable[][] values;

    /**
     * Custom constructor necessary for output formatting. Otherwise produces error.
     */
    public IntTwoDArrayWritable() {
        this.valueClass = IntWritable.class;
    }

    /**
     * Constructor sets the class being used.
     *
     * @param valueClass The class type used for internal structure.
     */
    public IntTwoDArrayWritable(Class valueClass) {
        this.valueClass = valueClass;
    }

    /**
     * Constructor which sets the class being used as well a the data in the two D array.
     *
     * @param valueClass The class type used for internal structure.
     * @param values The IntWritable two D array input data.
     */
    public IntTwoDArrayWritable(Class valueClass, IntWritable[][] values) {
        this(valueClass);
        this.values = values;
    }

    /**
     * Set the data to a new IntWritable two D array.
     *
     * @param values The IntWritable two D array input data.
     */
    public void set(IntWritable[][] values) { this.values = values; }

    /**
     * Returns the current IntWritable two D array data.
     * @return IntWritable two D array data currently in this Writable.
     */
    public IntWritable[][] get() { return this.values; }

    /**
     * Method that reads the fields in this custom Writable to be used after serialization.
     *
     * @param in DataInput which will be set in a new IntWritable two D array.
     * @throws IOException Returns default error.
     */
    public void readFields(DataInput in) throws IOException {
        // Construct the IntWritable two D array.
        this.values = new IntWritable[in.readInt()][];
        for (int i = 0; i < this.values.length; i++) {
            this.values[i] = new IntWritable[in.readInt()];
        }

        // Construct the values and them to the IntWritable two D array
        for (int i = 0; i < this.values.length; i++) {
            for (int j = 0; j < this.values[i].length; j++) {
                IntWritable value;
                try {
                    value = (IntWritable) this.valueClass.newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException(e.toString());
                }
                value.readFields(in);
                this.values[i][j] = value;
            }
        }
    }

    /**
     * Method that writes the IntWritable two D array data to a DataOutput.
     *
     * @param out DataOutput which will be filled with values from the IntWritable two D array.
     * @throws IOException Returns default error.
     */
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.values.length);
        for (int i = 0; i < this.values.length; i++) {
            out.writeInt(this.values[i].length);
        }
        for (int i = 0; i < this.values.length; i++) {
            for (int j = 0; j < this.values[i].length; j++) {
                this.values[i][j].write(out);
            }
        }
    }
}
