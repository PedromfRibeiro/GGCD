import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.*;

public class SeasonHits {

    // comparator for pairs (primaryTitle, rating)
    public static class MyComparator implements Comparator<Tuple2<String, Double>> {
        @Override
        public int compare(Tuple2<String, Double> value1, Tuple2<String, Double> value2) {
            //if (value1._2 > value2._2) return -1;
            //else return 1;
            return - Double.compare(value1._2, value2._2);
        }
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("SeasonHits");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse title.basics file to create pairs (tconst, startYear) only for movies
        JavaPairRDD<String, Tuple2<String, String>> title_basics = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Classes/Data/title.basics.tsv.bz2")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // ignore instances with missing values for the atribute startYear
                .filter(l -> !l[5].equals("\\N"))
                // create pairs (tconst, (primaryTitle, startYear))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(l[2], l[5])))
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

        // compute the best rated titles for each year
        List<Tuple2<String, Tuple2<String, Double>>> values = title_basics
                // join with RDD ratings, creating pairs (tconst, ((primaryTitle, startYear), rating))
                .join(ratings)
                // create pairs (startYear, (primaryTitle, rating))
                .mapToPair(p -> new Tuple2<>(p._2._1._2, new Tuple2<>(p._2._1._1, p._2._2)))
                // group by key, creating pairs (startYear, [(primaryTitle, rating), ...])
                .groupByKey()
                // sort values and get the best rated title, creating pairs (startYear, (primaryTitle, rating))
                .map(p -> {
                    List<Tuple2<String, Double>> list = Lists.newArrayList(p._2);
                    Collections.sort(list, new MyComparator());
                    Tuple2<String, Tuple2<String, Double>> bestRated = new Tuple2<>(p._1, list.get(0));
                    return bestRated;
                    })
                // create pairs (starYear, (primaryTitle, rating))
                .mapToPair(t -> new Tuple2<>(t._1, t._2))
                // sort pairs by key
                .sortByKey()
                // run the job
                .collect();

        // Show results
        for (Tuple2<String, Tuple2<String, Double>> value : values) {
            System.out.println("Year " + value._1 + ": title \'" + value._2._1 + "\' with a rating of " + value._2._2);
        }
    }
}
