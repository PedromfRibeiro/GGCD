import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.util.ArrayList;

public class Hits {

    public static void main(String[] args) {

        // spark configuration
        SparkConf conf = new SparkConf().setAppName("Hits");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse the file name.basics to create pairs (tconst, nconst), only for actors
        JavaPairRDD<String, String> actors = sc.textFile("hdfs:///name.basics.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("nconst"))
                // ignore non actors
                .filter(l -> l[4].contains("actor") || l[4].contains("actress"))
                // create pairs (nconst, [tconst, ...])
                .mapToPair(l -> new Tuple2<>(l[0], Arrays.asList(l[5].split(",")).iterator()))
                // flat to [(nconst, tconst), ...]
                .flatMapValues(l -> l)
                // create pairs (tconst, nconst)
                .mapToPair(l -> new Tuple2<>(l._2, l._1));

        // parse the file title.basics to create pairs (tconst, primaryTitle)
        JavaPairRDD<String, String> titles = sc.textFile("hdfs:///title.basics.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // create pairs (tconst, primaryTitle)
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        // parse the file title.ratings to create pairs (tconst, averageRating)
        JavaPairRDD<String, Double> ratings = sc.textFile("hdfs:///title.ratings.tsv.gz")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // create pairs (tconst, averageRating)
                .mapToPair(l -> new Tuple2<>(l[0], Double.parseDouble(l[1])));

        // join with ratings, creating pairs (tconst, (primaryTitle, averageRating))
        JavaPairRDD<String, Tuple2<String, Double>> titles_ratings = titles.join(ratings);

        // compute the top 10 best rated titles for each actor, creating pairs (nconst, [(title, averageRating), ...])
        List<Tuple2<String, List<Tuple2<String, Double>>>> results = actors
                // join with ratings, creating pairs (tconst, (nconst, (primaryTitle, averageRating)))
                .join(titles_ratings)
                // create pairs (nconst, (primaryTitle, averageRating))
                .mapToPair(l -> new Tuple2<>(l._2._1, new Tuple2<>(l._2._2._1, l._2._2._2)))
                // group by nconst, creating pairs (nconst, [(title, averageRating), ...])
                .groupByKey()
                // compute the top 10 best rated titles
                .mapToPair(p -> {
                    // sort titles
                    List<Tuple2<String, Double>> list = Lists.newArrayList(p._2);
                    Collections.sort(list, new Utils.MyComparatorD());
                    // get top 10 titles
                    if (list.size() >= 10) {
                        List<Tuple2<String, Double>> top = new ArrayList(list.subList(0, 10));
                        return new Tuple2<>(p._1, top);
                    }
                    else {
                        List<Tuple2<String, Double>> top = new ArrayList(list.subList(0, list.size()));
                        return new Tuple2<>(p._1, top);
                    }
                })
                // run the job
                .collect();

        // show results
        for (Tuple2<String, List<Tuple2<String, Double>>> value : results) {
            System.out.println(value._1 + ":");
            for (Tuple2<String, Double> title : value._2) {
                System.out.println("  > " + title._1 + " (average rating = " + title._2 + ")");
            }
        }

        // close spark context
        sc.close();
    }
}
