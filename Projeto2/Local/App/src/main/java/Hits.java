import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.List;
import java.util.Collections;
import java.util.ArrayList;

public class Hits {

    public static void main(String[] args) {

        // spark configuration
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Hits");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse the file title.principals to create pairs (tconst, nconst), only for actors
        JavaPairRDD<String, String> actors = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/title.principals.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // ignore non actors
                .filter(l -> l[3].equals("actor") || l[3].equals("actress"))
                // create pairs (tconst, nconst)
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        // parse the file title.basics to create pairs (tconst, primaryTitle)
        JavaPairRDD<String, String> titles = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/title.basics.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // create pairs (tconst, primaryTitle)
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        // parse the file title.ratings to create pairs (tconst, averageRating)
        JavaPairRDD<String, Double> ratings = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/title.ratings.tsv.gz")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // create pairs (tconst, averageRating)
                .mapToPair(l -> new Tuple2<>(l[0], Double.parseDouble(l[1])));

        // join with ratings, creating pairs (tconst, (primaryTitle, averageRating))
        JavaPairRDD<String, Tuple2<String, Double>> titles_ratings = titles.join(ratings);

        // compute the top 10 best rated titles for each actor, creating pairs (nconst, [(primaryTitle, averageRating), ...])
        List<Tuple2<String, List<Tuple2<String, Double>>>> results = actors
                // join with ratings, creating pairs (tconst, (nconst, (primaryTitle, averageRating)))
                .join(titles_ratings)
                // create pairs (nconst, (primaryTitle, averageRating))
                .mapToPair(p -> new Tuple2<>(p._2._1, new Tuple2<>(p._2._2._1, p._2._2._2)))
                // group by nconst, creating pairs (nconst, [(primaryTitle, averageRating), ...])
                .groupByKey()
                // compute the top 10 best rated titles
                .mapToPair(p -> {
                    // sort titles
                    List<Tuple2<String, Double>> list = Lists.newArrayList(p._2);
                    Collections.sort(list, new Utils.MyComparatorD());
                    // get top 10 titles
                    List<Tuple2<String, Double>> topTitles;
                    if (list.size() >= 10) topTitles = new ArrayList(list.subList(0, 10));
                    else topTitles = new ArrayList(list.subList(0, list.size()));
                    return new Tuple2<>(p._1, topTitles);
                })
                // run the job
                .collect();

        // show results
        for (Tuple2<String, List<Tuple2<String, Double>>> value : results) {
            System.out.println("\n" + value._1 + ":");
            for (Tuple2<String, Double> title : value._2) {
                System.out.println("  > " + title._1 + " (average rating = " + title._2 + ")");
            }
        }

        // close spark context
        sc.close();
    }
}
