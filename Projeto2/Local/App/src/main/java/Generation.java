import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class Generation {

    public static void main(String[] args) {

        // spark configuration
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Generation");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse the file title.principals to create pairs (nconst, number of titles)
        JavaPairRDD<String, Integer> numberTitles = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/title.principals.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // ignore non actors
                .filter(l -> l[3].equals("actor") || l[3].equals("actress"))
                // create pairs (nconst, tconst)
                .mapToPair(l -> new Tuple2<>(l[2], l[0]))
                // group by nconst, creating pairs (nconst, [tconst, ...])
                .groupByKey()
                // create pairs (nconst, number of titles)
                .mapToPair(p -> new Tuple2<>(p._1, Lists.newArrayList(p._2).size()));

        // parse the file name.basics to create pairs (nconst, (primaryName, decade))
        JavaPairRDD<String, Tuple2<String, String>> actorsInfo = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/name.basics.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("nconst"))
                // ignore missing values for the attribute birthYear
                .filter(l -> !l[2].equals("\\N"))
                // create pairs (nconst, (primaryName, birthYear))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(l[1], l[2])))
                // create pairs (nconst, (primaryName, decade))
                .mapToPair(p -> new Tuple2<>(p._1, new Tuple2<>(p._2._1, p._2._2.substring(0, p._2._2.length() - 1))));

        // compute the top 10 actors from the same generation, creating pairs (decade, [(primaryName, number of titles), ...])
        List<Tuple2<String, List<Tuple2<String, Integer>>>> results = numberTitles
                // join with generations, creating pairs (nconst, (number of titles, (primaryName, decade)))
                .join(actorsInfo)
                // create pairs (decade, (primaryName, number of titles))
                .mapToPair(p -> new Tuple2<>(p._2._2._2, new Tuple2<>(p._2._2._1, p._2._1)))
                // group by decade, creating pairs (decade, [(primaryName, number of titles), ...])
                .groupByKey()
                // compute the top 10 actors
                .mapToPair(p -> {
                    // sort actors
                    List<Tuple2<String, Integer>> actors = Lists.newArrayList(p._2);
                    Collections.sort(actors, new Utils.MyComparatorID());
                    // get top 10 actors
                    List<Tuple2<String, Integer>> topActors;
                    if (actors.size() >= 10) topActors = new ArrayList(actors.subList(0, 10));
                    else topActors = new ArrayList(actors.subList(0, actors.size()));
                    return new Tuple2<>(p._1, topActors);
                })
                // sort by decade
                .sortByKey()
                // jun the job
                .collect();

        // show results
        for (Tuple2<String, List<Tuple2<String, Integer>>> value : results) {
            System.out.println("\nDecade of " + value._1 + "0-9:");
            for (Tuple2<String, Integer> actor : value._2) {
                System.out.println("  > " + actor._1 + " with " + actor._2 + " titles");
            }
        }

        // close spark context
        sc.close();
    }
}
