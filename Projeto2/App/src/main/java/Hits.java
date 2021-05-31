import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.*;

public class Hits {

    public static class MyComparator implements Comparator<Tuple2<String, Double>> {
        @Override
        public int compare(Tuple2<String,Double> value1, Tuple2<String,Double> value2) {
            if (value1._2 > value2._2) return -1;
            else return 1;
        }
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("Hits");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse name.basics file to create pairs (tconst, nconst) only for actors
        JavaPairRDD<String, String> name_basics = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/name.basics.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("nconst"))
                // filter actors
                .filter(l -> l[4].contains("actor") || l[4].contains("actress"))
                // create list of pairs (nconst, [tconst, ...])
                .mapToPair(l -> new Tuple2<>(l[0], Arrays.asList(l[5].split(",")).iterator()))
                // flat pairs to [(nconst, tconst), ...]
                .flatMapValues(l -> l)
                // create pairs (tconst, nconst)
                .mapToPair(l -> new Tuple2<>(l._2, l._1))
                // caching
                .cache();

        // parse title.basics file to create pairs (tconst, primaryTitle) only for movies
        JavaPairRDD<String, String> title_basics = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/title.basics.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // filter movies
                .filter(l -> l[1].equals("movie"))
                // create pairs (tconst, primaryTitle)
                .mapToPair(l -> new Tuple2<>(l[0], l[2]))
                // caching
                .cache();

        // parse title.ratings file to create pairs (tconst, averageRating)
        JavaPairRDD<String, Double> ratings = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/title.ratings.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // create pairs (tconst, averageRating)
                .mapToPair(l -> new Tuple2<>(l[0], Double.parseDouble(l[1])))
                // caching
                .cache();

        // join RDDs title_basics and ratings
        JavaPairRDD<String, Tuple2<String, Double>> movie_ratings = title_basics.join(ratings); // (tconst, (primaryTitle, averageRating))

        // join RDDs name_basics and movie_ratings
        List<Tuple2<String, Iterable<Tuple2<String, Double>>>> values = name_basics.join(movie_ratings) // (tconst, (nconst, (primaryTitle, averageRating)))
                .mapToPair(l -> new Tuple2<>(l._2._1, new Tuple2<>(l._2._2._1, l._2._2._2)))            // (nconst, (primaryTitle, averageRating))
                .groupByKey()                                                                           // (nconst, [(title, averageRating), ...]
                .collect();

        // sort and show results
        for (Tuple2<String, Iterable<Tuple2<String, Double>>> value : values) {
            List<Tuple2<String, Double>> list = new ArrayList<>(Lists.newArrayList(value._2));
            Collections.sort(list, new MyComparator());

            System.out.println(value._1 + ":");
            if (list.size() >= 10) {
                for (int i = 0; i < 10; i++) {
                    System.out.println("  > " + list.get(i)._1 + " (average rating = " + list.get(i)._2 + ")");
                }
            }
            else {
                for (int i = 0; i < list.size(); i++) {
                    System.out.println("  > " + list.get(i)._1 + " (average rating = " + list.get(i)._2 + ")");
                }
            }
        }
    }
}
