import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.List;
import java.util.ArrayList;

public class Top10 {

    public static void main(String[] args) {

        long time = System.currentTimeMillis();

        // spark configuration
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Top10");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // compute the top 10 actors by number of titles, creating pairs (nconst, number of titles)
        List<Tuple2<String, Integer>> top10 = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/title.principals.tsv.gz")
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
                .mapToPair(p -> new Tuple2<>(p._1, Lists.newArrayList(p._2).size()))
                // compute the top 10 actors by number of titles
                .top(10, new Utils.MyComparatorIA());

        // create a list with the ids (nconst) of the top 10 actors
        List<String> top10Ids = new ArrayList<>();
        top10.forEach(t -> top10Ids.add(t._1));

        // compute the number of titles for the top 10 actors, creating pairs (primaryName, number of titles)
        List<Tuple2<String, Integer>> results = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/name.basics.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("nconst"))
                // ignore non top 10 actors
                .filter(l -> top10Ids.contains(l[0]))
                // create pairs (nconst, primaryName)
                .mapToPair(l -> new Tuple2<>(l[0], l[1]))
                // create pairs (primaryName, number of titles)
                .mapToPair(p -> {
                    // find the number of titles
                    Tuple2<String, Integer> tuple = top10.stream()
                            .filter(t -> t._1.equals(p._1))
                            .findAny()
                            .orElse(null);
                    return new Tuple2<>(p._2, tuple._2);
                })
                // create pairs (number of titles, primaryName)
                .mapToPair(p -> new Tuple2<>(p._2, p._1))
                // sort by number of titles, in descending order
                .sortByKey(false)
                // create pairs (primaryName, number of titles)
                .mapToPair(p -> new Tuple2<>(p._2, p._1))
                // run the job
                .collect();

        // show results
        for (Tuple2<String, Integer> value : results) {
            System.out.println(value._1 + " with " + value._2 + " titles");
        }

        // close spark context
        sc.close();

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
