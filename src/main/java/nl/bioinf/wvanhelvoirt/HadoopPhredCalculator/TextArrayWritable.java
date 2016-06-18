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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * TextArrayWritable
 *
 * A Custom Writable class based on the ArrayWritable, but uses Text's instead of Writable's.
 *
 * @author Wout van Helvoirt
 */
public class TextArrayWritable implements Writable {

    /** The class type. */
    private final Class valueClass;
    /** A Text array to be used internally. */
    private Text[] values;

    /**
     * Custom constructor necessary for output formatting. Otherwise produces error.
     */
    public TextArrayWritable() {
        this.valueClass = Text.class;
    }

    /**
     * Constructor sets the class being used.
     *
     * @param valueClass The class type used for internal structure.
     */
    public TextArrayWritable(Class valueClass) {
        this.valueClass = valueClass;
    }

    /**
     * Constructor which sets the class being used as well a the data in the Text array.
     *
     * @param valueClass The class type used for internal structure.
     * @param values     The Text array input data.
     */
    public TextArrayWritable(Class valueClass, Text[] values) {
        this(valueClass);
        this.values = values;
    }

    /**
     * Set the data to a new Text array.
     *
     * @param values The Text array input data.
     */
    public void set(Text[] values) {
        this.values = values;
    }

    /**
     * Returns the current Text array data.
     *
     * @return Text array data currently in this Writable.
     */
    public Text[] get() {
        return this.values;
    }

    /**
     * Method that reads the fields in this custom Writable to be used after serialization.
     *
     * @param in DataInput which will be set in a new Text array.
     * @throws IOException Returns default error.
     */
    public void readFields(DataInput in)
            throws IOException {

        // Construct the Text array, the values and add them to the Text array.
        this.values = new Text[in.readInt()];
        for (int i = 0; i < this.values.length; i++) {
            Text value;
            try {
                value = (Text) this.valueClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e.toString());
            }
            value.readFields(in);
            this.values[i] = value;
        }
    }

    /**
     * Method that writes the Text array data to a DataOutput.
     *
     * @param out DataOutput which will be filled with values from the Text array.
     * @throws IOException Returns default error.
     */
    public void write(DataOutput out)
            throws IOException {

        out.writeInt(this.values.length);
        for (int i = 0; i < this.values.length; i++) {
            this.values[i].write(out);
        }
    }
}
