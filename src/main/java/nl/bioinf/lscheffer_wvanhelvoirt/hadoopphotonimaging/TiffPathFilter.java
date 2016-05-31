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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * TiffPathFilter
 *
 * This class checks if paths are tiff files for use with Hadoop MapReduce.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class TiffPathFilter implements PathFilter {

    /** Regex string for filtering files. */
    private final String regex;

    /**
     * Constructor that sets regex to select only tiff files.
     */
    public TiffPathFilter() {
        this.regex = ".*\\.[Tt]+?[Ii]+?[Ff]+?[Ff]?";
    }

    /**
     * Override method that returns true if the input file path matches the regex.
     *
     * @param path The path of a file to be check by the filter.
     * @return boolean if file is tiff file.
     */
    @Override
    public boolean accept(Path path) {
        return path.toString().matches(regex);
    }
}
