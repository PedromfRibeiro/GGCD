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
        JavaPairRDD<String, Integer> sortedActors = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/title.principals.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // ignore non actors
                .filter(l -> l[3].contains("actor") || l[3].contains("actress"))
                // create pairs (nconst, tconst)
                .mapToPair(l -> new Tuple2<>(l[2], l[0]))
                // group by nconst, creating pairs (nconst, [tconst, ...])
                .groupByKey()
                // create pairs (nconst, number of titles)
                .mapToPair(p -> new Tuple2<>(p._1, Lists.newArrayList(p._2).size()));

        // parse the file name.basics to create pairs (nconst, decade)
        JavaPairRDD<String, String> generations = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/name.basics.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("nconst"))
                // ignore missing values for the attribute birthYear
                .filter(l -> !l[2].equals("\\N"))
                // create pairs (nconst, birthYear)
                .mapToPair(l -> new Tuple2<>(l[0], l[2]))
                // create pairs (nconst, decade)
                .mapToPair(p -> new Tuple2<>(p._1, p._2.substring(0, p._2.length() - 1)));

        // compute the top 10 actors from the same generation, creating pairs (decade, [(nconst, number of titles), ...])
        List<Tuple2<String, List<Tuple2<String, Integer>>>> results = sortedActors
                // join with generations, creating pairs (nconst, (number of titles, decade))
                .join(generations)
                // create pairs (decade, (nconst, number of titles))
                .mapToPair(p -> new Tuple2<>(p._2._2, new Tuple2<>(p._1, p._2._1)))
                // group by decade, creating pairs (decade, [(nconst, number of titles), ...])
                .groupByKey()
                // compute the top 10 actors
                .mapToPair(p -> {
                    List<Tuple2<String, Integer>> actors = Lists.newArrayList(p._2);
                    Collections.sort(actors, new Utils.MyComparatorID());
                    if (actors.size() >= 10) {
                        List<Tuple2<String, Integer>> top10 = new ArrayList(actors.subList(0, 10));
                        return new Tuple2<>(p._1, top10);
                    }
                    else {
                        List<Tuple2<String, Integer>> top10 = new ArrayList(actors.subList(0, actors.size()));
                        return new Tuple2<>(p._1, top10);
                    }
                })
                // sort by decade
                .sortByKey()
                // jun the job
                .collect();

        // show results
        for (Tuple2<String, List<Tuple2<String, Integer>>> value : results) {
            System.out.println("Decade of " + value._1 + "0-9:");
            for (Tuple2<String, Integer> actor : value._2) {
                System.out.println("  > " + actor._1 + " with " + actor._2 + " titles");
            }
        }

        // close spark context
        sc.close();
    }
}
