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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.ParseException;

/**
 * ArgumentParser
 *
 * This class parser arguments from the command-line to be used for single photon processing via Hadoop.
 *
 * @author Lonneke Scheffer and Wout van Helvoirt
 */
public class ArgumentParser {

    /**
     * @param args default arguments.
     */
    private final String[] args;

    /**
     * @param options will contain the main command line options (help, indir, outfile).
     */
    private final Options allOptions;

    /**
    * Constructor creating all the options for the command line parser.
    * @param args contains user's command line input.
    */
    public ArgumentParser(final String[] args) {
        this.args = args;
        this.allOptions = new Options();

        /* Make main options, infile is required. */
        Option help = Option.builder("h")
                .longOpt("help")
                .desc("Display help for this program.")
                .required(false)
                .build();
        Option jobname = Option.builder("n")
                .argName("JOBNAME")
                .hasArg()
                .longOpt("jobname")
                .required(true)
                .desc("Name of the job to be ran.")
                .build();
        Option indir = Option.builder("i")
                .argName("INDIR")
                .hasArg()
                .longOpt("indir")
                .required(true)
                .desc("Input directory containing TIFF files to proces.")
                .build();
        Option outfile = Option.builder("o")
                .argName("OUTDIR")
                .hasArg()
                .longOpt("outdir")
                .required(true)
                .desc("Output directory with TIFF file.")
                .build();


        /* Add main options to all available options. */
        allOptions.addOption(help);
        allOptions.addOption(jobname);
        allOptions.addOption(indir);
        allOptions.addOption(outfile);
    }

    /**
    * Function for retrieving user's command line input.
    * @return Option
    */
    public final List parseArguments() {

        CommandLineParser parser = new DefaultParser();
        CommandLine cmdArguments;
        List parsedArguments = new ArrayList();

        /* Try to parse arguments, otherwise give error message and print help. */
        try {
            cmdArguments = parser.parse(getAllOptions(), getArgs());

            /* If help option is given, print help message. */
            if (cmdArguments.hasOption("h")) {
                help();
            } else {
                parsedArguments.add(0, cmdArguments.getOptionValue("n"));
                parsedArguments.add(1, cmdArguments.getOptionValue("i"));
                parsedArguments.add(2, cmdArguments.getOptionValue("o"));
                return parsedArguments;
            }

        } catch (ParseException e) {
            System.out.println("A problem occured: " + e.getMessage() + "\n");
            help();
        }
        return null;
    }

    /**
    * Help function that prints usage and available parameters.
    * It uses the main class it's name for displaying usage.
    */
    public final void help() {

        /* Get the correct thread that is equal to the name of program */
        StackTraceElement[] stack = Thread.currentThread().getStackTrace();
        StackTraceElement main = stack[stack.length - 1];
        String mainClassName = main.getClassName().replaceAll(".*\\.", "");

        /* Use formatter to create correct help output text. */
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(100,
                "java -jar " + mainClassName + ".jar",
                "\nThis commandline java program can be used to proces singel photon data via Hadoop.\n",
                getAllOptions(),
                "\nCopyright (c) 2016 Lonneke Scheffer and Wout van Helvoirt",
                true);
        System.exit(0);
    }

    /**
    * Get arguments values.
    * @return args.
    */
    public final String[] getArgs() {
        return args;
    }

    /**
    * Get all option values.
    * @return allOptions.
    */
    public final Options getAllOptions() {
        return allOptions;
    }

    @Override
    public String toString() {
        return "User arguments: " + Arrays.toString(getArgs()) + ", Available options: " + getAllOptions();
    }
}