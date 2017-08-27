package com.tryit.spark.rdd.api;

/**
 * Created by agebriel on 12/12/16.
 *
 * http://www.datasalt.com/2015/12/a-scalable-groupbykey-and-secondary-sort-for-java-spark/
 * https://issues.apache.org/jira/browse/SPARK-3461
 * https://gist.github.com/pereferrera/710d717079b0e1e91bcf
 *
 */

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

/**
 * Current limitation of groupByKey in Spark is that the data for one key must fin in memory. This is in some cases not
 * acceptable. Right now, in order to overcome this limitation, it is possible to use a combination of commands
 * (partition + sort) and iterate over all the data of one partition. This class encapsulates this mechanism and also
 * offers the possibility of using secondary sort.
 * <p>
 * Relevant blog posts: <a href=
 * "http://apache-spark-user-list.1001560.n3.nabble.com/Mapping-Hadoop-Reduce-to-Spark-td13179.html#a13203"> this</a>,
 * <a href="http://codingjunkie.net/spark-secondary-sort/">this</a>
 *
 * @author pere
 *
 */
@SuppressWarnings({ "serial", "unchecked" })
public class ScalableGroupByKey
{
	/**
	 * The API version without secondary sort. It calls the version with secondary sort internally with a foo values
	 * comparator that always returns 0.
	 */
	public static <G extends Comparable<G>, T, K> JavaRDD<K> groupByKey(JavaPairRDD<G, T> rdd,
	                                                                    GroupByKeyHandler<G, T, K> handler) {
		//return groupByKeySecondarySort(rdd, (t1, t2) -> 0, handler);
		return groupByKeySecondarySort(rdd, new SerializableComparator(){
			@Override
			public int compare(Object o1, Object o2)
			{
				return 0;
			}
		}, handler);
	}

	/**
	 * The API version with secondary sort.
	 */
    public static <G extends Comparable<G>, T, K> JavaRDD<K> groupByKeySecondarySort(JavaPairRDD<G, T> rdd,
	                      SerializableComparator<T> secondarySort, GroupByKeyHandler<G, T, K> handler) {

		//JavaPairRDD<Tuple2<G, T>, Void> allInKey = rdd
		//	.mapToPair(tuple -> new Tuple2<Tuple2<G, T>, Void>(new Tuple2<>(tuple._1(), tuple._2()), Void.instance()));

		JavaPairRDD<Tuple2<G, T>, Void> allInKey = rdd
			.mapToPair(new PairFunction<Tuple2<G, T>, Tuple2<G, T>, Void>()
			{
				@Override
				public Tuple2<Tuple2<G, T>, Void> call(Tuple2<G, T> gtTuple2) throws Exception
				{
					return null;
				}
			});

		final int partitions = allInKey.partitions().size();
		return allInKey.repartitionAndSortWithinPartitions(
			new Partitioner() {
                   @Override
                   public int getPartition(Object obj) {
                       Tuple2<G, T> group = (Tuple2<G, T>) obj;
                       return Math.abs(group._1().hashCode()) % partitions;
                   }

                   @Override
                   public int numPartitions() {
                       return partitions;
                   }
               },

               //new GroupAndValueTupleComparator<>(secondarySort))
			   //.mapPartitions(i -> new IteratorIterable<>(new GroupIterator<>(i, handler)));
				new GroupAndValueTupleComparator(secondarySort))
				.mapPartitions(new FlatMapFunction<Iterator<Tuple2<Tuple2<G, T>, Void>>, K>()
				{
					@Override
					public Iterator<K> call(Iterator<Tuple2<Tuple2<G, T>, Void>> tuple2Iterator) throws Exception
					{
						return null;
					}
				});
	}

	

	/**
     * A comparator that sorts a tuple that conceptually has a group and a value. The group is assumed to be Comparable.
     * The comparator for the value can be provided freely via constructor. This class is specific to what we want to do
     * here and it is useful in the context of a groupBy where we might want to support arbitrary sorting over the
     * values (secondary sort).
     */
    private static class GroupAndValueTupleComparator<G extends Comparable<G>, T>
        implements Serializable, Comparator<Tuple2<G, T>>
    {

        private Comparator<T> elementComparator;

        public GroupAndValueTupleComparator(Comparator<T> elementComparator)
        {
            this.elementComparator = elementComparator;
        }

        @Override
        public int compare(Tuple2<G, T> o1, Tuple2<G, T> o2)
        {
            int comp = o1._1().compareTo(o2._1());
            if (comp == 0)
            {
                return elementComparator.compare(o1._2(), o2._2());
            }
            else
            {
                return comp;
            }
        }
    }

    /**
     * A simple wrapper over Object that implements a foo Object, like Hadoop's NullWritable.
     */
    public final static class Void
        extends Object
    {

        static Void theVoid = new Void();

        public static Void instance()
        {
            return theVoid;
        }
    }

    /**
     * We need this class due to this API problem: <a href="https://issues.apache.org/jira/browse/SPARK-3369">see
     * this</a>. This is a hack: we are assuming that Spark will only iterate once over the result (which happens to be
     * true).
     */
    public static class IteratorIterable<T>
        implements Iterable<T>
    {

        private final Iterator<T> iterator;
        private boolean consumed;

        public IteratorIterable(Iterator<T> iterator)
        {
            this.iterator = iterator;
        }

        @Override
        public Iterator<T> iterator()
        {
            if (consumed)
            {
                throw new IllegalStateException("Iterator already consumed");
            }
            consumed = true;
            return iterator;
        }
    }

    /**
     * This is the iterator that we return to Spark - here we do all the magic of detecting when a group starts and when
     * a group ends. So we provide another iterator to the end user, who doesn't need to care about detecting start and
     * end of a group.
     */
    private static class GroupIterator<G, T, K>
        implements Iterator<K>
    {

        private G currentGroup = null;
        private T currentElement = null;

        private Iterator<Tuple2<Tuple2<G, T>, Void>> sparkIterator;
        private GroupByKeyHandler<G, T, K> handler;

        public GroupIterator(Iterator<Tuple2<Tuple2<G, T>, Void>> sparkIterator, GroupByKeyHandler<G, T, K> handler)
        {
            this.sparkIterator = sparkIterator;
            this.handler = handler;
        }

        @Override
        public boolean hasNext()
        {
            return currentElement != null || sparkIterator.hasNext();
        }

        @Override
        public K next()
        {
            if (currentGroup == null)
            {
                // we didn't see any group in this partition yet
                Tuple2<Tuple2<G, T>, Void> tuple = sparkIterator.next();
                currentGroup = tuple._1()._1();
                currentElement = tuple._1()._2();
            }

            // iterator to be given to the user for this group
            Iterator<T> it = new Iterator<T>()
            {

                boolean hasNext = true;

                @Override
                public boolean hasNext()
                {
                    return hasNext;
                }

                @Override
                public T next()
                {
                    T toReturn = currentElement;

                    if (sparkIterator.hasNext())
                    {
                        Tuple2<Tuple2<G, T>, Void> tuple = sparkIterator.next();
                        G group = tuple._1()._1();
                        hasNext = group.equals(currentGroup);
                        currentElement = tuple._1()._2();
                        currentGroup = group;
                    }
                    else
                    {
                        currentElement = null;
                        hasNext = false;
                    }

                    return toReturn;
                }

	            @Override
	            public void remove()
	            {

	            }
            };

            K toReturn = handler.onGroup(currentGroup, it);
            // fully consume if the user didn't consume it all
            while (it.hasNext())
            {
                it.next();
            }
            return toReturn;
        }

        @Override
        public void remove()
        {

        }
    }

    

	/**
	 * A wrapper over Java Comparator that is Serializable, so we can safely implement lambdas.
	 */
	public static interface SerializableComparator<G>
		extends Comparator<G>, Serializable
	{
	}

	public static interface GroupByKeyHandler<G, T, K>
		extends Serializable
	{

		public K onGroup(G group, Iterator<T> values);
	}

	public static class ComparableImpl<T> implements Comparable<T>
	{
		@Override
		public int compareTo(T o)
		{
			return 0;
		}
	}
}
