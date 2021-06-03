import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.time.Year;
import java.util.List;

public class Base {

    public static void main(String[] args) {

        // spark configuration
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Base");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse the file title.principals to create pairs (tconst, nconst)
        JavaPairRDD<String, String> actors = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/title.principals.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // ignore non actors
                .filter(l -> l[3].contains("actor") || l[3].contains("actress"))
                // create pairs (tconst, nconst)
                .mapToPair(l -> new Tuple2<>(l[0], l[2]));

        // parse the file title.ratings to create pairs (tconst, averageRating)
        JavaPairRDD<String, Double> ratings = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/title.ratings.tsv.gz")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // create pairs (tconst, averageRating)
                .mapToPair(l -> new Tuple2<>(l[0], Double.parseDouble(l[1])));

        // compute the number of titles and the mean of the ratings from the titles of each actor, creating pairs (nconst, (number of titles, meanRating))
        JavaPairRDD<String, Tuple2<Integer, Double>> titlesInfo = actors
                // join with ratings, creating pairs (tconst, (nconst, averageRating))
                .join(ratings)
                // create pairs (nconst, averageRating)
                .mapToPair(p -> new Tuple2<>(p._2._1, p._2._2))
                // group by nconst
                .groupByKey()
                // compute the mean of the ratings from the titles, creating pairs (nconst, (number of titles, meanRating))
                .mapToPair(p -> {
                    List<Double> averageRatings = Lists.newArrayList(p._2);
                    double sum = 0;
                    for (Double averageRating : averageRatings) {
                        sum += averageRating;
                    }
                    double mean = sum / averageRatings.size();
                    return new Tuple2<>(p._1, new Tuple2<>(averageRatings.size(), mean));
                });

        // parse the file name.basics to create pairs (nconst, (primaryName, age))
        JavaPairRDD<String, Tuple2<String, Integer>> actorsInfo = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/name.basics.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("nconst"))
                // ignore non actors
                .filter(l -> l[4].contains("actor") || l[4].contains("actress"))
                // ignore missing values for the attribute birthYear
                .filter(l -> !l[2].equals("\\N"))
                // create pairs (nconst, (primaryName, age))
                .mapToPair(l -> {
                    // calculate the age
                    int age = 0;
                    if (!l[3].equals("\\N")) age = Integer.parseInt(l[3]) - Integer.parseInt(l[2]);
                    else age = Year.now().getValue() - Integer.parseInt(l[2]);
                    return new Tuple2<>(l[0], new Tuple2<>(l[1], age));
                });

        // compute the name, age and mean average rating from the films of each actor, creating pairs (nconst, ((primaryName, age), (number of titles, meanRating))))
        List<Tuple2<String, Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Double>>>> results = actorsInfo
                // join with meanRatings, creating pairs (nconst, ((primaryName, age), (number of titles, meanRating))))
                .join(titlesInfo)
                // run the job
                .collect();

        // show results
        for (Tuple2<String, Tuple2<Tuple2<String, Integer>, Tuple2<Integer, Double>>> value : results) {
            System.out.println("\n" + value._1 + ":");
            System.out.println("  > name: " + value._2._1._1);
            System.out.println("  > age: " + value._2._1._2 + " years");
            System.out.println("  > number of titles: " + value._2._2._1);
            System.out.println("  > mean rating from titles: " + value._2._2._2);
        }

        // close spark context
        sc.close();
    }
}
