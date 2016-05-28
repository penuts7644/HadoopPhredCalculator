# Hadoop Photon Imaging #

---------------------

### What is this repository for? ###

* Authors: Lonneke Scheffer & Wout van Helvoirt
* Version: 1.0
* This project is a modified version of the PhotonImaging plug-in for ImageJ, which is able to process single photon event data, by locating the center point of each photon and create a combined grayscale image with all found photons per pixel mapped to the correct pixel value.

### How do I get set up? ###

* You need some sort of Hadoop enabled cluster and a Hadoop client from which you can run this program. More information about Hadoop can be found [here](http://hadoop.apache.org)
* This software requires at least [Java 8](https://www.oracle.com/downloads/index.html) to function.

### How do I use this application? ###

The jar file can be run via the Hadoop client's command-line. With the command 'hadoop jar' you can run a jar file. The command consists out of:

* The path to the jar file, like '~/HadoopPhotonImaging.jar'.
* The main class address (nl.bioinf.lscheffer_wvanhelvoirt.hadoopphotonimaging.ParallelPhotonImageProcessor)
* An input file or files in an directory, like '-D input.dir=[files in directory or file goes here]'
* An output directory, like '-D output.dir=[directory to save result goes here]'.
* Optional is to set the job name with '-D mapreduce.job.name=[job name goes here]', default value is 'PhotonImageProcess'.
