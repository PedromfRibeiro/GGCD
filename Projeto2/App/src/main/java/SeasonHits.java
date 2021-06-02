import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.List;
import java.util.Collections;

public class SeasonHits {

    public static void main(String[] args) {

        // spark configuration
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SeasonHits");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse the file title.basics to create pairs (tconst, startYear)
        JavaPairRDD<String, Tuple2<String, String>> titles = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/title.basics.tsv.gz")
                // split attributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // ignore missing values for the attribute startYear
                .filter(l -> !l[5].equals("\\N"))
                // create pairs (tconst, (primaryTitle, startYear))
                .mapToPair(l -> new Tuple2<>(l[0], new Tuple2<>(l[2], l[5])));

        // parse the file title.ratings to create pairs (tconst, averageRating)
        JavaPairRDD<String, Double> ratings = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/title.ratings.tsv.gz")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // create pairs (tconst, averageRating)
                .mapToPair(l -> new Tuple2<>(l[0], Double.parseDouble(l[1])));

        // compute the best rated titles for each year, creating pairs (startYear, (Title, averageRating))
        List<Tuple2<String, Tuple2<String, Double>>> results = titles
                // join with ratings, creating pairs (tconst, ((primaryTitle, startYear), averageRating))
                .join(ratings)
                // create pairs (startYear, (primaryTitle, averageRating))
                .mapToPair(p -> new Tuple2<>(p._2._1._2, new Tuple2<>(p._2._1._1, p._2._2)))
                // group by startYear, creating pairs (startYear, [(primaryTitle, averageRating), ...])
                .groupByKey()
                // compute the best rated title, creating pairs (startYear, (primaryTitle, averageRating))
                .mapToPair(p -> {
                    // sort pairs (primaryTitle, averageRating)
                    List<Tuple2<String, Double>> list = Lists.newArrayList(p._2);
                    Collections.sort(list, new Utils.MyComparatorD());
                    // get the best rated title
                    return new Tuple2<>(p._1, list.get(0));
                    })
                // sort by startYear
                .sortByKey()
                // run the job
                .collect();

        // show results
        for (Tuple2<String, Tuple2<String, Double>> value : results) {
            System.out.println("Year " + value._1 + ": title \'" + value._2._1 + "\' with a rating of " + value._2._2);
        }

        // close spark context
        sc.close();
    }
}
