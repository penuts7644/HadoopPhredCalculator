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

import ij.ImagePlus;
import ij.gui.PolygonRoi;
import ij.gui.Roi;
import ij.gui.Wand;
import ij.io.Opener;
import ij.plugin.filter.RankFilters;
import ij.process.ImageProcessor;
import ij.process.ShortProcessor;
import org.apache.hadoop.io.IntWritable;
import java.awt.Polygon;
import java.awt.Rectangle;
import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * PhotonImageProcessor
 *
 * This class is able to process a stack containing single photon events data and create a combined hi-res image.
 * Each light point within the image (based on user given tolerance value) is being processed as photon. Each photon
 * has a center that can be calculated in a fast or a more accurate way. There are two accurate calculations available.
 * One to create a higher resolution image with four times the amount of pixels (sub-pixel resolution) or one with
 * normal resolution. Photons are being counted and mapped to the correct pixel values to create a 16-bit image.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class PhotonImageProcessor {

    /** The ImageProcessor. */
    private ImageProcessor ip;
    /** A matrix for counting photons. */
    private int[][] photonCountMatrix;
    /** Noise tolerance, default is 100. */
    private double tolerance;
    /** This boolean tells whether the user wants to perform preprocessing. */
    private boolean preprocessing;
    /** The output method (fast/accurate/sub-pixel resolution) is set to fast. */
    private String method;

    /**
     * Constructor.
     */
    public PhotonImageProcessor(ByteArrayInputStream bais, int tolerance, String method, boolean preprocessing) {
        this.ip = new Opener().openTiff(bais, "tiff_image").getProcessor();
        this.photonCountMatrix = new int[this.ip.getWidth()][this.ip.getHeight()];
        this.tolerance = tolerance;
        this.method = method;
        this.preprocessing = preprocessing;

        if (this.tolerance < 0) {
            this.tolerance = 0;
        }
    }

    /**
     * Executed method when selected.
     *
     * Run method gets executed when setup is finished and when the user selects this class via plug-ins in Fiji. Run
     * method needs to be overridden.
     */
    public void run() {
        Polygon rawCoordinates;

        // Preprocess the current slice.
        if (this.preprocessing) {
            this.preprocessImage(this.ip);
        }

        // Find the photon coordinates.
        rawCoordinates = this.findPhotons(this.ip);

        // If previewing enabled, show found maxima's on slice.
        if (this.method.equals("Fast")) {
            this.processPhotonsFast(rawCoordinates);
        } else {
            // Calculating the auto threshold takes relatively long so this function is only called once per image.
            //float autoThreshold = ip.getAutoThreshold();

            if (this.method.equals("Accurate")) {
                processPhotonsAccurate(this.ip, rawCoordinates);
            } else { // this.method equals "Subpixel resolution"
                processPhotonsSubPixel(this.ip, rawCoordinates);
            }
        }
    }

    /**
     * This method is called when processing photons using the 'fast' method.
     * All photons are added to the photon count matrix, without altering.
     *
     * @param rawCoordinates a polygon containing the coordinates as found by MaximumFinder
     */
    private void processPhotonsFast(final Polygon rawCoordinates) {
        // Loop through all raw coordinates and add them to the count matrix.
        for (int i = 0; i < rawCoordinates.npoints; i++) {
            this.photonCountMatrix[rawCoordinates.xpoints[i]]
                                  [rawCoordinates.ypoints[i]]++;
        }
    }

    /**
     * This method is called when processing photons using the 'accurate' method.
     * The exact coordinates are calculated, and then floored and added to the count matrix.
     *
     * @param ip the ImageProcessor of the current image slice
     * @param rawCoordinates a polygon containing the coordinates as found by MaximumFinder
     */
    private void processPhotonsAccurate(final ImageProcessor ip, final Polygon rawCoordinates) {
        for (int i = 0; i < rawCoordinates.npoints; i++) {
            // Loop through all raw coordinates, calculate the exact coordinates,
            // floor the coordinates, and add them to the count matrix.
            double[] exactCoordinates = this.calculateExactCoordinates(rawCoordinates.xpoints[i],
                                                                       rawCoordinates.ypoints[i], ip);
            this.photonCountMatrix[(int) exactCoordinates[0]]
                                  [(int) exactCoordinates[1]]++;
        }
    }

    /**
     * This method is called when processing photons using the 'subpixel resolution' method.
     * The exact coordinates are calculated, and then multiplied by two and added to the count matrix.
     *
     * @param ip the ImageProcessor of the current image slice
     * @param rawCoordinates a polygon containing the coordinates as found by MaximumFinder
     */
    private void processPhotonsSubPixel(final ImageProcessor ip, final Polygon rawCoordinates) {
        for (int i = 0; i < rawCoordinates.npoints; i++) {
            // Loop through all raw coordinates, calculate the exact coordinates,
            // double the coordinates, and add them to the count matrix.
            double[] exactCoordinates = this.calculateExactCoordinates(rawCoordinates.xpoints[i],
                                                                       rawCoordinates.ypoints[i],
                                                                       ip);
            this.photonCountMatrix[(int) (exactCoordinates[0] * 2)]
                                  [(int) (exactCoordinates[1] * 2)]++;
        }
    }


    /**
     * Preprocess the images. For instance: despeckling the images to prevent false positives.
     *
     * @param ip ImageProcessor.
     */
    private void preprocessImage(final ImageProcessor ip) {
        // Perform 'despeckle' using RankFilters.
        SilentRankFilters r = new SilentRankFilters();
        r.rank(ip, 1, RankFilters.MEDIAN);
    }

    /**
     * Find the photons in the current image using MaximumFinder, and return their approximate coordinates.
     *
     * @param ip ImageProcessor.
     * @return Polygon with all maxima points found.
     */
    private Polygon findPhotons(final ImageProcessor ip) {
        int[][] coordinates;

        // Find the maxima using MaximumFinder
        SilentMaximumFinder maxFind = new SilentMaximumFinder();
        Polygon maxima = maxFind.getMaxima(ip, this.tolerance, true);

        coordinates = new int[2][maxima.npoints];
        coordinates[0] = maxima.xpoints; // X coordinates
        coordinates[1] = maxima.ypoints; // y coordinates

        return maxima;
    }

    /**
     * Calculate the exact sub-pixel positions of the photon events at the given coordinates.
     *
     * @param xCor Original x coordinate as found by MaximumFinder.
     * @param yCor Original y coordinate as found by MaximumFinder.
     * @param ip ImageProcessor.
     * @return The new calculated coordinates.
     */
    private double[] calculateExactCoordinates(final int xCor, final int yCor, final ImageProcessor ip) {
        // Wand MUST BE created here, otherwise wand object might be used for multiple photons at the same time.
        Wand wd = new Wand(ip);
        double[] subPixelCoordinates = new double[2];

        // Outline the center of the photon using the wand tool.
        //wd.autoOutline(xCor, yCor, autoThreshold, Wand.FOUR_CONNECTED);
        wd.autoOutline(xCor, yCor, this.tolerance, Wand.FOUR_CONNECTED);

        // Draw a rectangle around the outline.
        Rectangle rect = new PolygonRoi(wd.xpoints, wd.ypoints, wd.npoints, Roi.FREEROI).getBounds();

        // Check if the newly found coordinates are reasonable.
        // (If the original midpoint is too dark compared to the background,
        // the whole image might be selected by the wand tool, if the tolerance is too high.)
        if (rect.height == ip.getHeight() || rect.width > ip.getWidth()) {
            // If the width and height of the rectangle are too big, use the original coordinates.
            subPixelCoordinates[0] = xCor;
            subPixelCoordinates[1] = yCor;
        } else {
            // Otherwise, return the centers of the found rectangles as new coordinates.
            subPixelCoordinates[0] = rect.getCenterX();
            subPixelCoordinates[1] = rect.getCenterY();
        }

        return subPixelCoordinates;
    }

    /**
     * This method generates and displays the final image from the photonCountMatrix.
     */
    public void createOutputImage() {

        // Create new ShortProcessor for output image with matrix data and it's width and height.
        ShortProcessor sp = new ShortProcessor(this.photonCountMatrix.length, this.photonCountMatrix[0].length);
        sp.setIntArray(this.photonCountMatrix);

        // Add the amount of different values in array.
        List<Integer> diffMatrixCount = new ArrayList<>();
        for (int[] photonCountMatrix1 : this.photonCountMatrix) {
            for (int photonCountMatrix2 : photonCountMatrix1) {
                if (!diffMatrixCount.contains(photonCountMatrix2)) {
                    diffMatrixCount.add(photonCountMatrix2);
                }
            }
        }

        // Use 0 as min and largest value in the matrix as max for grayscale mapping.
        sp.setMinAndMax(0, (diffMatrixCount.size() - 2)); // Pixel mapping uses blocks.


        // Create new output image with title.
        ImagePlus outputImage = new ImagePlus("Photon Count Image", sp);

        // Make new image window in ImageJ and set the window visible.
        // ImageWindow outputWindow = new ImageWindow(outputImage);
        // outputWindow.setVisible(true);
    }

    /**
     * This method gets the ip.
     *
     * @return ip ImageProcessor.
     */
    public ImageProcessor getIp() {
        return ip;
    }

    /**
     * This method gets the photonCountMatrix.
     *
     * @return photonCountMatrix int[][].
     */
    public int[][] getPhotonCountMatrix() {
        return photonCountMatrix;
    }

    /**
     * This method returns the given tolerance.
     *
     * @return tolerance double.
     */
    public double getTolerance() {
        return tolerance;
    }

    /**
     * This method returns true if preprocessing is enabled.
     *
     * @return preprocessing boolean.
     */
    public boolean isPreprocessing() {
        return preprocessing;
    }

    /**
     * This method returns the selected method.
     *
     * @return method String.
     */
    public String getMethod() {
        return method;
    }
}
