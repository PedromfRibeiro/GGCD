import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.sparkproject.guava.collect.Lists;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import java.util.Collections;
import java.util.Arrays;

public class TopGenres {

    // flatten an ArrayList of ArrayLists to an ArrayList
    public static ArrayList<String> flattenList(ArrayList<ArrayList<String>> nestedList) {
        ArrayList<String> list = new ArrayList<>();
        nestedList.forEach(list::addAll);
        return list;
    }

    // comparator for pairs (genre, count)
    public static class MyComparator implements Comparator<Tuple2<String, Integer>> {
        @Override
        public int compare(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
            if (value1._2 > value2._2) return -1;
            else return 1;
        }
    }

    // count the occurences of each String in a List
    public static Iterator<Tuple2<String, Integer>> countOccurrences(List<String> list) {
        Set<String> distinct = new HashSet<>(list);
        List<Tuple2<String, Integer>> tuples = new ArrayList<>();
        for (String s: distinct) {
            tuples.add(new Tuple2<>(s, Collections.frequency(list, s)));
        }
        Collections.sort(tuples, new MyComparator());
        return tuples.iterator();
    }

    public static void main(String[] args) {

        // Spark configuration
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TopGenres");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // parse title.basics file to create pairs (startYear, genre)
        JavaPairRDD<String, Tuple2<String, Integer>> title_basics = sc.textFile("file:///Users/goncalo/Documents/University/GGCD/Spark/Data/title.basics2.tsv")
                // split atributes
                .map(l -> l.split("\t"))
                // ignore header
                .filter(l -> !l[0].equals("tconst"))
                // ignore instances with missing values for the atribute startYear
                .filter(l -> !l[5].equals("\\N"))
                // ignore instances with missing values for the atribute genres
                .filter(l -> !l[8].equals("\\N"))
                // create pairs (startYear, [genre, ...])
                .mapToPair(l -> new Tuple2<>(l[5], Arrays.asList(l[8].split(",")).iterator()))
                // create pairs (decade, [genre, ...])
                .mapToPair(p -> new Tuple2<>(p._1.substring(0, p._1.length() - 1), Lists.newArrayList(p._2)))
                // group by decade, creating pairs (decade, [[genre, ...], [genre, ...]])
                .groupByKey()
                // create pairs (decade, [genre1, genre2, ...])
                .mapToPair(p -> new Tuple2<>(p._1, flattenList(Lists.newArrayList(p._2))))
                // create pairs (decade, [(genre, count), ...], sorted by count
                .mapToPair(p -> new Tuple2<>(p._1, countOccurrences(p._2)))
                // only keep the first value
                .mapToPair(p -> new Tuple2<>(p._1, p._2.next()))
                // sort pairs by key
                .sortByKey();

        // run the job
        List<Tuple2<String, Tuple2<String, Integer>>> values = title_basics.collect();

        // Show results
        for (Tuple2<String, Tuple2<String, Integer>> value : values) {
            System.out.println("Decade of " + value._1 + "0-9: " + value._2._1 + " with " + value._2._2 + " titles" );
        }
    }
}