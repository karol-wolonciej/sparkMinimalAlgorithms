# algorytmy minimalne
[Link do artykułu](https://www.researchgate.net/publication/262218295_Minimal_MapReduce_algorithms)

Nootebok w zepellinie z zaimplementowanymi algorytmami minimalnymi w sparku i scali 2:

- MapReduce
- Terasort
- Sliding Aggregation
- Perfect Balance Sort
- Prefix Sum
- Ranking


  trait MapReduce extends java.io.Serializable {
      this: Settings =>
      import org.apache.spark.Partitioner
      
      // force user to use partitioner for appriopriate type of key with static typing
      abstract class genericPartitioner[K] extends Partitioner { 
          override def getPartition(key: Any) = convert(key)
          override def numPartitions = numberOfMachines
          
          def convert(key: Any): Int
      }
      
      
      implicit object minimalAlgorithmsPartitioner extends genericPartitioner[Int] {
          def convert(key: Any): Int = { 
              val machineId = key.asInstanceOf[Int]
              assert(0 <= machineId && machineId <= lastMachineId, s"bad machineId $machineId")
              machineId
          }
      }
      
      
      def mapReduce[K : ClassTag, T1, T2 : ClassTag, R : ClassTag](dataset: RDD[T1], 
                                  mapFun: (T1) => (K, T2), 
                                  reduce: ((K, Iterable[T2])) => R)
                                  (implicit partitioner: genericPartitioner[K]): RDD[R] = {
                                      
          return dataset.map(mapFun).groupByKey(partitioner).map(reduce)
      }
      
      
      def mapReduce[K : ClassTag, T1, T2 : ClassTag, R : ClassTag](dataset: RDD[T1], 
                                  mapFun: T1 => List[(K, T2)], 
                                  reduce: ((K, Iterable[T2])) => R)
                                  (implicit partitioner: genericPartitioner[K], 
                                  d: DummyImplicit): RDD[R] = {
                                      
          return dataset.flatMap(mapFun).groupByKey(partitioner).map(reduce)
      }
      
      
      def mapReduce[K : ClassTag, T1, T2 : ClassTag, R : ClassTag](dataset: RDD[T1], 
                                  mapFun: T1 => (K, T2), 
                                  reduce: ((K, Iterable[T2])) => List[R])
                                  (implicit partitioner: genericPartitioner[K], 
                                  d1: DummyImplicit,
                                  d2: DummyImplicit): RDD[R] = {
          
          return dataset.map(mapFun).groupByKey(partitioner).flatMap(reduce)
      }
                                      
                                      
      def mapReduce[K : ClassTag, T1, T2 : ClassTag, R : ClassTag](dataset: RDD[T1], 
                                  mapFun: T1 => List[(K, T2)], 
                                  reduce: ((K, Iterable[T2])) => List[R])
                                  (implicit partitioner: genericPartitioner[K], 
                                  d1: DummyImplicit,
                                  d2: DummyImplicit,
                                  d3: DummyImplicit): RDD[R] = {
                                      
          return dataset.flatMap(mapFun).groupByKey(partitioner).flatMap(reduce)
      }
  }
