# Hadoop PHRED Calculator #

---------------------

### What is this repository for? ###

* Authors: Wout van Helvoirt
* Version: 1.0
* This project processes a FastQ file and creates a text file containing the average PHRED score per base via Hadoop.

### How do I get set up? ###

* You need a Hadoop enabled cluster and a Hadoop client from which you can run this program. More information about Hadoop can be found [Here](http://hadoop.apache.org).
* This software requires at least [Java 7](https://www.oracle.com/downloads/index.html) to function.
* The source has been written in IntelliJ IDEA 2016.

### How do I use this application? ###

The jar file can be run via the Hadoop client's command-line. With the command below, you can run the program.

    yarn jar HadoopPhredCalculator.jar nl.bioinf.wvanhelvoirt.HadoopPhredCalculator.ParallelPhredCalculator -D input.files=[input file/files] -D output.dir=[output directory] -D mapreduce.job.name=[job name]

The command consists out of:

* Main Hadoop yarn command.
* The path to the jar file.
* The main class address.
* The input file or files in an directory.
* An output directory.
* Optional is to set the job name (mapreduce.job.name). Default value is 'PhotonImageProcess'.
