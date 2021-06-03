package grupo5;

import com.google.common.collect.Iterables;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Top10
 */
public class Top10 {
    /**
     * Tuple2's Comparator
     */
    public static class MyComparator implements Serializable, Comparator<Tuple2<Integer, String>> {
        @Override
        public int compare(Tuple2<Integer, String> t1, Tuple2<Integer, String> t2) {
            return t1._1.compareTo(t2._1);
        }
    }

    public static void main(String[] args) {
        long time = System.currentTimeMillis();

        // Spark configuration
        SparkConf conf = new SparkConf().setAppName("Top10");
        JavaSparkContext sc = new JavaSparkContext(conf);
        /**
         * !Initial processing of the "title.principals.tsv" file
         **0 - string tconst     - alphanumeric unique identifier of the title
         **1 - int    ordering   – a number to uniquely identify rows for a given titleId
         **2 - string nconst     - alphanumeric unique identifier of the name/person
         **3 - string category   - the category of job that person was in
         **4 - string job        - the specific job title if applicable, else '\N'
         **5 - string characters - the name of the character played if applicable, else '\N'
         */
        List<Tuple2<Integer, String>> top10 = sc.textFile("hdfs://namenode:9000/data/title.principals.tsv")
                                                .map(l -> l.split("\t"))
                                                .filter(l -> !l[0].equals("tconst"))
                                                .mapToPair(l -> new Tuple2<>(l[2], l[0]))
                                                .groupByKey()
                                                .mapToPair(p -> new Tuple2<>(Iterables.size(p._2), p._1))
                                                .top(10, new MyComparator());

        List<String> top10ActorsIds = new ArrayList<>();
        top10.forEach(t -> top10ActorsIds.add(t._2));
        /**
         * ! Initial processing of the "name.basics.tsv" file
         * * 0 - string   nconst            - alphanumeric unique identifier of the name/person
         * * 1 - string   primaryName       – name by which the person is most often credited
         * * 2 - YYYY     birthYear
         * * 3 - YYYY     deathYear         – if not applicable = '\N'
         * * 4 - string[] primaryProfession – the top-3 professions of the person
         * * 5 - int[]    knownForTitles    – titles the person is known for
         */
        Map<String, String> names = sc.textFile("hdfs://namenode:9000/data/name.basics.tsv")
                                      .map(l -> l.split("\t"))
                                      .filter(l -> !l[0].equals("nconst") && !l[1].equals("primaryName") && top10ActorsIds.contains(l[0]))
                                      .mapToPair(l -> new Tuple2<>(l[0], l[1]))
                                      .collectAsMap();
        // Output result
        System.out.println("\nTop 10:\n");
        for (Tuple2<Integer, String> t : top10) {
            System.out.println(names.get(t._2) + " : " + t._1);
        }
        System.out.println();
        // Close spark context
        sc.close();

        System.out.println("\nTime: " + (System.currentTimeMillis() - time) + " ms");
    }
}
