# Hadoop Photon Imaging #

---------------------

### What is this repository for? ###

* Authors: Lonneke Scheffer & Wout van Helvoirt
* Version: 1.0
* This project is a modified version of the PhotonImaging plug-in for ImageJ. The Hadoop version is able to process single photon event data, by locating the center point of each photon. The reduce stage combines all the two D arrays from the mappers into one 16-bit greyscale image. Each pixel contains the amount of found photons and are mapped to the correct pixel value.

### How do I get set up? ###

* You need a Hadoop enabled cluster and a Hadoop client from which you can run this program. More information about Hadoop can be found [Here](http://hadoop.apache.org).
* This software requires at least [Java 8](https://www.oracle.com/downloads/index.html) to function.
* The source has been written in IntelliJ IDEA 2016.

### How do I use this application? ###

The jar file can be run via the Hadoop client's command-line. With the command below, you can run the program.

    hadoop jar HadoopPhotonImaging.jar nl.bioinf.lscheffer_wvanhelvoirt.hadoopphotonimaging.ParallelPhotonImageProcessor -D input.files=[input file/files] -D output.dir=[output directory]

The command consists out of:

* Main Hadoop command.
* The path to the jar file.
* The main class address.
* The input file or files in an directory.
* An output directory.
* Optional is to set the job name with '-D mapreduce.job.name'. When not given, default value is 'PhotonImageProcess'.
