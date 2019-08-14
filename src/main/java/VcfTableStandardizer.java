import com.google.api.services.bigquery.Bigquery;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class VcfTableStandardizer {

    private BigQuery bigquery = null;
    private String datasetName = null;

    final private int writeBufSize = 512 * 1024; // buffer size in bytes


    public VcfTableStandardizer(String credentialFilePath) {
        BigQueryOptions.Builder bqb = BigQueryOptions.newBuilder();
        try {
            bqb.setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream(credentialFilePath)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.bigquery = bqb.build().getService();
        //this.bigquery = BigQueryOptions.getDefaultInstance().getService();
    }

    private String getProjectId() {
        return bigquery.getOptions().getProjectId();
    }

    private boolean pathExists(String path) {
        return Files.exists(Paths.get(path));
    }

    public void wellderlyCSV(String inputPath, String outputPath) throws IOException {
        if (!pathExists(inputPath)) {
            throw new IOException("Input path does not exist: " + inputPath);
        } else if (!pathExists(outputPath)) {
            throw new IOException("Output path does not exist: " + outputPath);
        }

        Path iPath = Paths.get(inputPath);
        int tempLimit = 1, tempCount = 0;
        List<Path> items = Files.list(iPath).collect(Collectors.toList());
        final int fileCount = items.size();
        int fileIndex = 0;
        //Files.walk(iPath).forEach(path -> {
        for (Path path : items) {
            String pathString = path.toString();
            System.out.println(String.format(
                    "Processing input file (%d of %d) %s",
                    ++fileIndex,
                    fileCount,
                    path.toAbsolutePath().toString()));
            if (Files.isDirectory(path)) {
                // skip
                System.err.println(String.format("%s is a directory", pathString));
                continue; // for stream, change to return
            } else if (!pathString.endsWith(".csv")) {
                System.err.println(String.format("%s does not end is .csv", pathString));
                continue;
            }

            //String[] columnNames = {"ID","QUAL","FILTER","INFO","reference_name","start_position","end_position","reference_bases","alternate_bases"};
            CSVFormat csvFormat = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase(); // ignore column name casing
            CSVParser parser = CSVParser.parse(new BufferedReader(new FileReader(new File(path.toString()))), csvFormat);

            final CSVPrinter csvPrinter;
            try {
                String newPath = getOutFilePath(path.toAbsolutePath().toString(), outputPath);
                System.out.println("Writing to " + newPath);
                File outFile = new File(newPath);
                FileWriter fwriter = new FileWriter(outFile);
                BufferedWriter bwriter = new BufferedWriter(fwriter);
                csvPrinter = CSVFormat.DEFAULT
                        .withHeader("REFERENCE_NAME", "START_POSITION", "END_POSITION", "REFERENCE_BASES", "ALTERNATE_BASES", "MINOR_AF", "ALLELE_COUNT")
                        .print(bwriter);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
            // loop over input rows in this file
            parser.forEach(inputRow -> {
                String info = inputRow.get("INFO");
                String referenceName = inputRow.get("reference_name");
                Long startPosition = Long.valueOf(inputRow.get("start_position"));
                Long endPosition = Long.valueOf(inputRow.get("end_position"));
                String referenceBases = inputRow.get("reference_bases");
                String alternateBases = inputRow.get("alternate_bases");

                // parse info column
                //ArrayList<String> freqList = new ArrayList<String>();
                String[] infoTerms = info.split(";");
                // according to wellderly spec, AF is guaranteed to be 8th term in info text
                final int afInfoIndex = 7;
                final int acInfoIndex = 8; // allele count is 9th term
                String[] refAltFrequencyInfo = infoTerms[afInfoIndex].split(",");
                String[] refAltCountInfo = infoTerms[acInfoIndex].split(",");
                if (refAltCountInfo.length != refAltFrequencyInfo.length) {
                    throw new RuntimeException(String.format(
                            "Malformed allele count/frequency fields. Number of terms did not match (ac: %d, af: %d)",
                            refAltCountInfo.length, refAltFrequencyInfo.length));
                }
                // wellderly includes the major allele frequency in the AF list, skip it
                String[] frequencies = new String[refAltFrequencyInfo.length - 1];
                String[] counts = new String[refAltCountInfo.length - 1];
                // extract frequency number from term like "A-0.005"
                for (int i = 1; i < refAltFrequencyInfo.length; i++) {
                    String dfi = refAltFrequencyInfo[i];
                    String dci = refAltCountInfo[i];
                    // for non called alleles, wellderly uses "-" to indicate the alt bases
                    // so need to only look at last dash in dfi term
                    int lastFreqDashIndex = dfi.lastIndexOf("-");
                    if (lastFreqDashIndex <= 0) {
                        throw new RuntimeException("Malformed allele frequency: " + inputRow.toString() + "\n" + infoTerms[afInfoIndex]);
                    }
                    String[] fiTerms = {dfi.substring(0, lastFreqDashIndex), dfi.substring(lastFreqDashIndex + 1)};

                    int lastCountDashIndex = dfi.lastIndexOf("-");
                    if (lastCountDashIndex <= 0) {
                        throw new RuntimeException("Malformed allele count: " + inputRow.toString() + "\n" + infoTerms[acInfoIndex]);
                    }
                    String[] ciTerms = {dfi.substring(0, lastCountDashIndex), dci.substring(lastCountDashIndex + 1)};


                    frequencies[i - 1] = fiTerms[1];
                    counts[i - 1] = ciTerms[1];
                }

                // determine if alternate_bases match count of frequencies
                String[] altBaseArr = alternateBases.split(",\\s*");
                if (altBaseArr.length != frequencies.length || altBaseArr.length != counts.length) {
                    throw new RuntimeException(String.format("Number of alt bases did not match frequency or allele count in info terms: %s\n"
                                    + "Alt bases: %s\nFrequencies: %s\nCounts: %s",
                            infoTerms[afInfoIndex] + ";" + infoTerms[acInfoIndex],
                            altBaseArr.length, frequencies.length, counts.length));
                }

                // Add rows to insertAll builder
                for (int i = 0; i < frequencies.length; i++) {
                    // insert row into new table for this alternate allele
                    try {
                        //csvPrinter.printRecord(referenceName, startPosition, endPosition, referenceBases, altBaseArr[i], frequencies[i]);
                        csvPrintSync(csvPrinter, referenceName, startPosition, endPosition, referenceBases, altBaseArr[i], frequencies[i], counts[i]);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            });

            csvPrinter.flush();
            csvPrinter.close();
            // temporarily limit input
            //if (++tempCount >= tempLimit) {
            //    break;
            //}
        }
        //);

    }


    public void thousandGenomesCSV(String inputPath, String outputPath) throws IOException {
        if (!pathExists(inputPath)) {
            throw new IOException("Input path does not exist: " + inputPath);
        } else if (!pathExists(outputPath)) {
            throw new IOException("Output path does not exist: " + outputPath);
        }

        int tempLimit = 1, tempCount = 0;
        final boolean limit = false;

        Path iPath = Paths.get(inputPath);
        List<Path> items = Files.list(iPath).collect(Collectors.toList());
        final int fileCount = items.size();
        int fileIndex = 0;
        //Files.walk(iPath).forEach(path -> {
        for (Path path : items) {
            String pathString = path.toString();
            System.out.println(String.format(
                    "Processing input file (%d of %d) %s",
                    ++fileIndex,
                    fileCount,
                    path.toAbsolutePath().toString()));
            if (Files.isDirectory(path)) {
                // skip
                System.err.println(String.format("%s is a directory", pathString));
                continue; // for stream, change to return
            } else if (!pathString.endsWith(".csv")) {
                System.err.println(String.format("%s does not end is .csv", pathString));
                continue;
            }

            CSVFormat csvFormat = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase(); // ignore column name casing
            CSVParser parser = CSVParser.parse(new BufferedReader(new FileReader(new File(path.toString()))), csvFormat);

            final CSVPrinter csvPrinter;
            try {
                String newPath = getOutFilePath(path.toAbsolutePath().toString(), outputPath);
                System.out.println("Writing to " + newPath);
                File outFile = new File(newPath);
                FileWriter fwriter = new FileWriter(outFile);
                BufferedWriter bwriter = new BufferedWriter(fwriter);
                csvPrinter = CSVFormat.DEFAULT
                        .withHeader("REFERENCE_NAME", "START_POSITION", "END_POSITION", "REFERENCE_BASES", "ALTERNATE_BASES", "MINOR_AF", "ALLELE_COUNT")
                        .print(bwriter);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            final Pattern copyNumberAltPattern = Pattern.compile("^<CN[0-9]+>$");

            // loop over input rows in this file
            parser.forEach(inputRow -> {
                String minorAF = inputRow.get("af");
                String referenceName = inputRow.get("reference_name");
                Long startPosition = Long.valueOf(inputRow.get("start_position"));
                Long endPosition = Long.valueOf(inputRow.get("end_position"));
                String referenceBases = inputRow.get("reference_bases");
                String alternateBases = inputRow.get("alternate_bases");
                String alleleCount = inputRow.get("ac");

                // ignore copy number variants
                Matcher copyNumberMatcher = copyNumberAltPattern.matcher(alternateBases.trim());
                if (copyNumberMatcher.find()) {
                    // is a copy number variant, not very useful for this analysis, skip
                    //System.out.println("Variant was a copy number: " + alternateBases);
                    return;
                }

                try {
                    csvPrintSync(csvPrinter, referenceName, startPosition, endPosition, referenceBases, alternateBases, minorAF, alleleCount);
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }

            });

            csvPrinter.flush();
            csvPrinter.close();
            // temporarily limit input
            if (limit && ++tempCount >= tempLimit) {
                break;
            }
        }
    }



    private synchronized void csvPrintSync(CSVPrinter p, Object... o) throws IOException {
        p.printRecord(o);
    }

    private String getOutFilePath(String inputFilePath, String outputDirectory) {
        // get basename of file
        if (inputFilePath.endsWith("/")) {
            inputFilePath = inputFilePath.substring(0, inputFilePath.length() - 1);
        }
        if (!outputDirectory.endsWith("/")) {
            outputDirectory += "/";
        }
        String[] pathTerms = inputFilePath.split("/");
        String fileName = pathTerms[pathTerms.length - 1] + ".mini";
        fileName = outputDirectory + fileName;
        return fileName;
        //String baseName = fileName.split("\\.(?=[^\\.]+$)")[0] + ".mini";
    }


    /*public void wellderly() throws InterruptedException, IOException {
        final String originalTable = "gbsc-gcp-project-annohive-dev.swarm.GRCh37_Wellderly_wellderly_all_vcf";
        final String newProject = "gbsc-gcp-project-annohive-dev";
        final String newDataset = "swarm";
        final String newTable = "wellderly";
        final String readQuery = String.format(
                "SELECT reference_name, start_position, end_position, reference_bases, alternate_bases, info"
                + " FROM `%s`"
                // temporary filter for testing
                + " WHERE reference_bases = \"ACA\"",
                originalTable);
        TableResult tr = runSimpleQuery(readQuery);

        // Get reference to table and builder for an insertAll operation
        Table table = bigquery.getTable(TableId.of(newProject, newDataset + "." + newTable));
        InsertAllRequest.Builder insertBuilder = InsertAllRequest.newBuilder(table);

        CSVPrinter csvPrinter = null;
        try {
            FileWriter fwriter = new FileWriter(new File("wellderly.csv"));
            BufferedWriter bwriter = new BufferedWriter(fwriter, 1024 * 1024); // 1 MiB buffer
            csvPrinter = CSVFormat.DEFAULT
                    .withHeader("REFERENCE_NAME", "START_POSITION", "END_POSITION", "REFERENCE_BASES", "ALTERNATE_BASES", "AF")
                    .print(bwriter);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // Loop through rows read from original table
        for (FieldValueList row : tr.iterateAll()) {
            String referenceName = row.get("reference_name").getStringValue();
            Long startPosition = row.get("start_position").getLongValue();
            Long endPosition = row.get("end_position").getLongValue();
            String referenceBases = row.get("reference_bases").getStringValue();
            String alternateBases = row.get("alternate_bases").getStringValue();
            String info = row.get("info").getStringValue();

            // parse info column
            String[] frequencies = null;
            // capture everything from AF= through next non-specified character (in kvp format, should be a semicolon)
            Pattern alleleFrequencyKvpPattern = Pattern.compile("AF=([0-9\\.e\\-\\+,]+)");
            //Pattern p = Pattern.compile("(AF)");
            Matcher m = alleleFrequencyKvpPattern.matcher(info);
            if (m.find()) {
                String afBlock = m.group(1);
                frequencies = afBlock.split(",\\s*");
                System.out.println(String.join(", ", frequencies));
            } else {
                throw new RuntimeException("No AF values found in INFO text");
            }

            // determine if alternate_bases match count of frequencies
            String[] altBaseArr = alternateBases.split(",\\s*");
            if (altBaseArr.length != frequencies.length) {
                throw new RuntimeException(String.format("Alt base count (%s) did not match frequency count (%s)",
                        altBaseArr.length, frequencies.length));
            }

            // Add rows to insertAll builder
            for (int i = 0; i < frequencies.length; i++) {
                // insert row into new table for this alternate allele
                //Map<String,Object> newRow = new HashMap<>();
                //newRow.put("reference_name", referenceName);
                //newRow.put("start_position", startPosition);
                //newRow.put("end_position", endPosition);
                //newRow.put("reference_bases", referenceBases);
                //newRow.put("alternate_bases", altBaseArr[i]);
                //newRow.put("af", frequencies[i]);
                //insertBuilder.addRow(newRow);
                csvPrinter.printRecord(referenceName, startPosition, endPosition, referenceBases, altBaseArr[i], frequencies[i]);
            }

        }
        csvPrinter.flush();
        csvPrinter.close();

//        InsertAllResponse insertResponse = bigquery.insertAll(insertBuilder.build());
//
//        if (insertResponse.hasErrors()) {
//            insertResponse.getInsertErrors().forEach((id, errors) -> {
//                errors.forEach((error) -> {
//                    System.out.println(error.toString());
//                });
//            });
//        }
    }*/


    public void gnomadCSV(String inputPath, String outputPath) throws IOException {
        if (!pathExists(inputPath)) {
            throw new IOException("Input path does not exist: " + inputPath);
        } else if (!pathExists(outputPath)) {
            throw new IOException("Output path does not exist: " + outputPath);
        }

        Path iPath = Paths.get(inputPath);
        int tempLimit = 25, tempCount = 0;
        // set to true to limit to tempLimit
        final boolean limit = false;

        List<Path> items = Files.list(iPath).collect(Collectors.toList());
        final int fileCount = items.size();
        int fileIndex = 0;
        //Files.walk(iPath).forEach(path -> {
        for (Path path : items) {
            String pathString = path.toString();
            System.out.println(String.format(
                    "Processing input file (%d of %d) %s",
                    ++fileIndex,
                    fileCount,
                    path.toAbsolutePath().toString()));
            if (Files.isDirectory(path)) {
                // skip
                System.err.println(String.format("%s is a directory", pathString));
                continue; // for stream, change to return
            } else if (!pathString.endsWith(".csv")) {
                System.err.println(String.format("%s does not end is .csv", pathString));
                continue;
            }

            CSVFormat csvFormat = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .withIgnoreHeaderCase(); // ignore column name casing
            CSVParser parser = CSVParser.parse(new BufferedReader(new FileReader(new File(path.toString()))), csvFormat);

            final CSVPrinter csvPrinter;
            String newPath = getOutFilePath(path.toAbsolutePath().toString(), outputPath);
            System.out.println("Writing to " + newPath);
            try {
                File outFile = new File(newPath);
                FileWriter fwriter = new FileWriter(outFile);
                BufferedWriter bwriter = new BufferedWriter(fwriter);
                csvPrinter = CSVFormat.DEFAULT
                        .withHeader("REFERENCE_NAME", "START_POSITION", "END_POSITION", "REFERENCE_BASES", "ALTERNATE_BASES", "MINOR_AF", "ALLELE_COUNT")
                        .print(bwriter);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }

            // for finding AF=.... substring in info field
            final Pattern alleleFrequencyKvpPattern = Pattern.compile("AF=([0-9\\.e\\-\\+,]+)");
            // for finding AF_raw=... substring in info field
            final Pattern afRawKvpPattern = Pattern.compile("AF_raw=([0-9\\.e\\-\\+,]+)");
            // for finding AC=.... substring in info field
            final Pattern alleleCountKvpPattern = Pattern.compile("AC=([0-9,]+)");

            // loop over input rows in this file
            parser.forEach(inputRow -> {
                String info = inputRow.get("INFO");
                String referenceName = inputRow.get("reference_name");
                Long startPosition = Long.valueOf(inputRow.get("start_position"));
                Long endPosition = Long.valueOf(inputRow.get("end_position"));
                String referenceBases = inputRow.get("reference_bases");
                String alternateBases = inputRow.get("alternate_bases");

                // parse info column
                String[] frequencies = null, counts = null;
                // capture everything from AF= through next non-specified character (in kvp format, should be a semicolon)
                Matcher afMatcher = alleleFrequencyKvpPattern.matcher(info);
                Matcher acMatcher = alleleCountKvpPattern.matcher(info);
                String afBlock, acBlock;
                if (!afMatcher.find()) {
                    throw new RuntimeException("No AF values found in INFO text");
                }
                if (!acMatcher.find()) {
                    throw new RuntimeException("No AC values found in INFO text");
                }
                //if (afMatcher.find()) {
                afBlock = afMatcher.group(1);
                acBlock = acMatcher.group(1);
                String[] frequenciesTemp = afBlock.split(",\\s*");
                String[] countsTemp = acBlock.split(",\\s*");
                if (frequenciesTemp.length != countsTemp.length) {
                    throw new RuntimeException("Number of AF and AC terms did not match in info field: " + info);
                }
                // fix abnormalities in scientific notation
                frequencies = new String[frequenciesTemp.length];
                counts = new String[countsTemp.length];
                for (int i = 0; i < frequenciesTemp.length; i++) {
                    String freq = frequenciesTemp[i];
                    String count = countsTemp[i];
                    if (freq.equals(".")) {
                        // skip this variant, return from lambda
                        return;
                    }
                    try {
                        // use plain decimal instead of scientific notation format
                        BigDecimal b = new BigDecimal(freq);
                        frequencies[i] = b.toPlainString();
                        //frequencies[i] = freq;
                    } catch (NumberFormatException e) {
                        Matcher mRaw = afRawKvpPattern.matcher(info);
                        System.err.println("Failed to parse numbers from AF block " + afBlock + "\n"
                                + inputRow.toString() + "\n"
                                + "qual: " + inputRow.get("qual"));
                        if (mRaw.find()) {
                            String afRawBlock = mRaw.group(1);
                            System.out.println("af_raw: " + afRawBlock);
                        } else {
                            System.out.println("af_raw: not found");
                        }
                        throw e;
                    }
                    try {
                        Integer iObj = Integer.parseInt(count);
                        counts[i] = iObj.toString();
                    } catch (NumberFormatException e) {
                        System.err.println("Failed to parse integer from AC block " + acBlock + "\n" + inputRow.toString());
                        throw e;
                    }
                    //frequencies[i] = freq;
                } // end loop through frequencies
                /*} else {
                    throw new RuntimeException("No AF values found in INFO text");
                }*/

                // determine if alternate_bases match count of frequencies
                String[] altBaseArr = alternateBases.split(",\\s*");
                if (altBaseArr.length != frequencies.length) {
                    throw new RuntimeException(String.format("Alt base count (%s) did not match frequency count (%s)",
                            altBaseArr.length, frequencies.length));
                }

                // Add rows to insertAll builder
                for (int i = 0; i < frequencies.length; i++) {
                    // insert row into new table for this alternate allele
                    try {
                        //csvPrinter.printRecord(referenceName, startPosition, endPosition, referenceBases, altBaseArr[i], frequencies[i]);
                        csvPrintSync(csvPrinter, referenceName, startPosition, endPosition, referenceBases, altBaseArr[i], frequencies[i], counts[i]);
                    } catch (IOException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                    }
                }
            }); // end forEach line

            csvPrinter.flush();
            csvPrinter.close();

            // temporarily limit input
            if (limit && ++tempCount >= tempLimit) {
                break;
            }
        } // end loop over filenames
    }

    /*public void gnomad() throws InterruptedException {
        final String originalTable = "gbsc-gcp-project-annohive-dev.swarm.GRCh37_gnomAD_gnomad_genomes_sites_all_vcf";
        final String newProject = "gbsc-gcp-project-annohive-dev";
        final String newDataset = "swarm";
        final String newTable = "gnomad";

        final String readQuery = String.format(
                "SELECT reference_name, start_position, end_position, reference_bases, alternate_bases, info"
                + " FROM `%s`"
                // temporary filter for testing
                + " WHERE reference_bases = \"AC\"",
                originalTable);
        TableResult tr = runSimpleQuery(readQuery);

        // Get reference to table and builder for an insertAll operation
        Table table = bigquery.getTable(TableId.of(newProject, newDataset + "." + newTable));
        InsertAllRequest.Builder insertBuilder = InsertAllRequest.newBuilder(table);

        for (FieldValueList row : tr.iterateAll()) {
            String referenceName = row.get("reference_name").getStringValue();
            Long startPosition = row.get("start_position").getLongValue();
            Long endPosition = row.get("end_position").getLongValue();
            String referenceBases = row.get("reference_bases").getStringValue();
            String alternateBases = row.get("alternate_bases").getStringValue();
            String info = row.get("info").getStringValue();

            // parse info column
            String[] frequencies = null;
            // capture everything from AF= through next non-specified character (in kvp format, should be a semicolon)
            Pattern alleleFrequencyKvpPattern = Pattern.compile("AF=([0-9\\.e\\-\\+,]+)");
            //Pattern p = Pattern.compile("(AF)");
            Matcher m = alleleFrequencyKvpPattern.matcher(info);
            if (m.find()) {
                String afBlock = m.group(1);
                frequencies = afBlock.split(",\\s*");
                System.out.println(String.join(", ", frequencies));
            } else {
                throw new RuntimeException("No AF values found in INFO text");
            }

            // determine if alternate_bases match count of frequencies
            String[] altBaseArr = alternateBases.split(",\\s*");
            if (altBaseArr.length != frequencies.length) {
                throw new RuntimeException(String.format("Alt base count (%s) did not match frequency count (%s)",
                        altBaseArr.length, frequencies.length));
            }

            // Add rows to insertAll builder
            for (int i = 0; i < frequencies.length; i++) {
                // insert row into new table for this alternate allele
                Map<String,Object> newRow = new HashMap<>();
                newRow.put("reference_name", referenceName);
                newRow.put("start_position", startPosition);
                newRow.put("end_position", endPosition);
                newRow.put("reference_bases", referenceBases);
                newRow.put("alternate_bases", altBaseArr[i]);
                newRow.put("af", frequencies[i]);
                insertBuilder.addRow(newRow);
            }

        }
        InsertAllResponse insertResponse = bigquery.insertAll(insertBuilder.build());

        if (insertResponse.hasErrors()) {
            insertResponse.getInsertErrors().forEach((id, errors) -> {
                errors.forEach((error) -> {
                    System.out.println(error.toString());
                });
            });
        }
    }*/


//    public TableResult runSimpleQuery(String sql) throws InterruptedException {
//        QueryJobConfiguration queryConfig =
//                QueryJobConfiguration.newBuilder(sql).build();
//
//        //Table table = bigquery.getTable("");
//        //table.
//
//        // Create a job ID
//        JobId jobId = JobId.of(UUID.randomUUID().toString());
//        return bigquery.query(queryConfig);
//        /*Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());
//
//        queryJob = queryJob.waitFor();
//
//        if (queryJob == null) {
//            throw new RuntimeException("Job does not exist!");
//        } else if (queryJob.getStatus().getError() != null) {
//            // You can also look at queryJob.getStatus().getExecutionErrors() for all
//            // errors, not just the latest one.
//            throw new RuntimeException(queryJob.getStatus().getError().toString());
//        }
//
//        // Get the results.
//        //QueryResponse response = bigquery.getQueryResults(jobId);
//        return queryJob.getQueryResults();
//        */
//    }

}
