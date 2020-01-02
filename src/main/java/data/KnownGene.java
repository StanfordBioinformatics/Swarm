package data;

import app.dao.client.BigQueryClient;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class KnownGene {
    BigQueryClient bigQueryClient = new BigQueryClient("swarm", "../../gcp.json");
    public static void main(String[] args) throws FileNotFoundException {
        String fileName = "knownGene.txt";
        Scanner scany = new Scanner(new File(fileName));

    }
}
