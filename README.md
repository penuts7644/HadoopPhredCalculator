# Hadoop Phred Calculator #

---------------------

### About this project ###

* Authors: Wout van Helvoirt
* Version: 1.0
* This project processes a FastQ file and creates a text file containing the average phred score per base via Hadoop.

### Getting set up ###

* You need a Hadoop enabled cluster and a Hadoop client from which you can run this program. More information about
Hadoop can be found [Here](http://hadoop.apache.org).
* This software requires at least [Java 7](https://www.oracle.com/downloads/index.html) to function.
* The source has been written in IntelliJ IDEA 2016 and uses Maven for package management.

### How to use this application ###

The jar file can be run via the Hadoop client's command-line. With the command below, you can run the program.

    yarn jar HadoopPhredCalculator.jar nl.bioinf.wvanhelvoirt.HadoopPhredCalculator.ParallelPhredCalculator
    -D input.files=[input file/files]
    -D output.dir=[output directory]
    -D mapreduce.job.name=[job name]
    -D reads.per.map=[amount of reads per mapper]
    -D ascii.base=[base ascii value for phred score correcting]

The command consists out of:

* Main Hadoop yarn command, path to the jar file and main class address.
* Required: The input file or files in an directory.
* Required: An output directory were output files should be writen.
* Optional: Set the job name (mapreduce.job.name). Default value is 'PhredCalculator'.
* Optional: Set the amount of reads (4 lines) per mapper (reads.per.map). Default value is 2000.
* Optional: Set the ascii base value for correcting phred scores (ascii.base). Default value is 64.

### Troubleshooting ###

If you run want to run the Hadoop job using a Macintosh machine, you could get the following error:

    Exception in thread "main" java.io.IOException: Mkdirs failed to create /var/folders/1k/799h3b_s4pd87bg9d2mfv7k00000gn/T/hadoop-unjar7265077405644854771/META-INF/license
        at org.apache.hadoop.util.RunJar.ensureDirectory(RunJar.java:128)
        at org.apache.hadoop.util.RunJar.unJar(RunJar.java:104)
        at org.apache.hadoop.util.RunJar.unJar(RunJar.java:81)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:209)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:136)

This error can be fixed by removing 'META-INF/LICENSE' (note the capitals) from the jar file. This can be done by
executing the command below in the same directory where the jar file is located.

    zip -d HadoopPhredCalculator.jar META-INF/LICENSE

### My use case ###

I used one fairly small fastq file of 3.55 Gb. Each mapper receives a part of the fastq file and processes the reads.
The phred scores for each read per base are converted to ascii values, corrected and combined. The reduce stage combines
all the mapper outputs to one file containing per base the average phred score.