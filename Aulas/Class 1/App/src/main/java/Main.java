public class Main {

    public static void main(String[] args) {

        TSV tsv = new TSV();

        try {
            System.out.println("Parsing file title.basics.tsv ...");
            tsv.parseTitleBasics(args[0]);
            System.out.println("Parsing done!\n");

            System.out.println("Computing the top 10 most popular genres ...");
            tsv.topGenres();
            System.out.println("\nComputing done!\n");

            System.out.println("Parsing file title.principals.tsv ...");
            tsv.parseTitlePrincipals(args[1]);
            System.out.println("Parsing done!\n");

            System.out.println("Computing the list of titles identifiers for each person ...");
            tsv.computeTitleIdentifiers();
            System.out.println("\nComputing done!\n");
        }
        catch (Exception e) {
            System.out.println(e);
        }
    }
}