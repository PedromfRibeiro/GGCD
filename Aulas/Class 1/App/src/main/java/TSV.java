import java.io.*;
import java.util.Collections;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.commons.compress.compressors.CompressorStreamFactory;

public class TSV {

    // Variables
    private HashMap<String, String[]> titlesGenres;
    private HashMap<String, ArrayList<String>> titlesIdentifiers;

    // Constructor
    public TSV() {
        this.titlesGenres = new HashMap<String, String[]>();
        this.titlesIdentifiers = new HashMap<String, ArrayList<String>>();
    }

    // Parse file title.basics
    public void parseTitleBasics(String file) throws Exception {

        CompressorStreamFactory csf = new CompressorStreamFactory();
        BufferedReader tsvFile = new BufferedReader(new InputStreamReader(csf.createCompressorInputStream(new BufferedInputStream(new FileInputStream(file)))));

        // Skip header
        String row = tsvFile.readLine();

        while ((row = tsvFile.readLine()) != null) {
            String[] data = row.split("\t");
            String[] genres = data[8].split(",");
            this.titlesGenres.put(data[0], genres);
            row = tsvFile.readLine();
        }

        tsvFile.close();
    }

    // Parse file title.principals
    public void parseTitlePrincipals(String file) throws Exception {

        CompressorStreamFactory csf = new CompressorStreamFactory();
        BufferedReader tsvFile = new BufferedReader(new InputStreamReader(csf.createCompressorInputStream(new BufferedInputStream(new FileInputStream(file)))));

        // Skip header
        String row = tsvFile.readLine();

        while ((row = tsvFile.readLine()) != null) {
            String[] data = row.split("\t");
            String tconst = data[0];
            String nconst = data[2];
            ArrayList<String> titles;
            if (this.titlesIdentifiers.get(nconst) != null) titles = this.titlesIdentifiers.get(nconst);
            else titles = new ArrayList<String>();
            titles.add(tconst);
            this.titlesIdentifiers.put(nconst, titles);
            row = tsvFile.readLine();
        }

        tsvFile.close();
    }

    // Compute the top 10 most popular genres from title.basics
    public void topGenres() {

        HashMap<String, Integer> genresCount = new HashMap<String, Integer>();

        // Populate genresCount
        for (String[] array : this.titlesGenres.values()) {
            for (String genre : array) {
                if (genresCount.containsKey(genre)) genresCount.put(genre, genresCount.get(genre) + 1);
                else genresCount.put(genre, 1);
            }
        }

        // Initialize auxiliar arraylists
        ArrayList<String> topGenres = new ArrayList<>(10);
        ArrayList<Integer> counts = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            topGenres.add(i, "");
            counts.add(i, 0);
        }

        // Populate auxiliar arraylists
        for (Map.Entry<String, Integer> e : genresCount.entrySet()) {
            String genre = e.getKey();
            int count = e.getValue();

            if (!genre.equals("\\N")) {

                if (count > counts.get(9)) {
                    topGenres.add(9, genre);
                    counts.add(9, count);
                }

                for (int i = 9; i > 0; i--) {
                    if (counts.get(i) > counts.get(i-1)) {
                        Collections.swap(topGenres, i, i-1);
                        Collections.swap(counts, i, i-1);
                    }
                    else break;
                }
            }
        }

        // Print top 10 genres
        for (int i = 0; i < 10; i++) {
            System.out.println("#" + (i + 1) + ": " + topGenres.get(i) + " -> " + counts.get(i));
        }
    }

    // Compute the list of titles identifiers for each person, ordered by person identifier
    public void computeTitleIdentifiers() {

        Map<String, ArrayList<String>> orderedMap = new TreeMap<>(this.titlesIdentifiers);

        for (String nconst : orderedMap.keySet()) {
            System.out.println("\nnconst = " + nconst + ":");
            ArrayList<String> titles = this.titlesIdentifiers.get(nconst);
            for (String tconst : titles) {
                System.out.println(" - " + tconst);
            }
        }
    }
}