import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.List;
import java.util.Arrays;

public class TopGenres {

    public static void main(String[] args) {

        // spark configuration
        SparkConf conf = new SparkConf().setAppName("TopGenres");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // compute the most popular genre for each decade, creating pairs (decade, genre)
        List<Tuple2<String, Tuple2<String, Integer>>> decades = sc.textFile("hdfs:///title.basics.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // ignore missing values for the attributes startYear and genres
                .filter(l -> !l[5].equals("\\N") &&  !l[8].equals("\\N"))
                // create pairs (startYear, [genre, ...])
                .mapToPair(l -> new Tuple2<>(l[5], Arrays.asList(l[8].split(",")).iterator()))
                // create pairs (decade, [genre, ...])
                .mapToPair(p -> new Tuple2<>(p._1.substring(0, p._1.length() - 1), Lists.newArrayList(p._2)))
                // group by decade, creating pairs (decade, [[genre, ...], [genre, ...]])
                .groupByKey()
                // create pairs (decade, [genre, ...])
                .mapToPair(p -> new Tuple2<>(p._1, Utils.flattenList(Lists.newArrayList(p._2))))
                // create pairs (decade, [(genre, count), ...], sorted by count
                .mapToPair(p -> new Tuple2<>(p._1, Utils.countOccurrences(p._2)))
                // only keep the first value
                .mapToPair(p -> new Tuple2<>(p._1, p._2.next()))
                // sort by decade
                .sortByKey()
                // run the job
                .collect();

        // show results
        for (Tuple2<String, Tuple2<String, Integer>> decade : decades) {
            System.out.println("Decade of " + decade._1 + "0-9: " + decade._2._1 + " with " + decade._2._2 + " titles");
        }

        // close spark context
        sc.close();
    }
}