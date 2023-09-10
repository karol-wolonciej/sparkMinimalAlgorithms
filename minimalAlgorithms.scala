import org.apache.spark.rdd.RDD
import scala.reflect.ClassTag

trait Settings {
    type RDD[T] = org.apache.spark.rdd.RDD[T]
    type DataFrame = org.apache.spark.sql.DataFrame
    type MachineId = Int
    type Rank = Int
    
    val numberOfMachines = 10
    val firstMachineId = 0
    val lastMachineId = numberOfMachines-1
}

object Conversions {
    implicit val int2OptionInt: Int => Option[Int] = n => Some(n)
    implicit val long2OptionInt: Long => Option[Int] = n => Some(n.toInt)
}

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

object MinimalALgorithms extends MapReduce with Settings with java.io.Serializable {

    import org.apache.spark.Partitioner
    
    def getMachineId(): MachineId = {
        import org.apache.spark.TaskContext
        TaskContext.getPartitionId()
    }
    
    implicit val int2OptionInt: Int => Option[Int] = n => Some(n)
    implicit val long2OptionInt: Long => Option[Int] = n => Some(n.toInt)

    
    def terasort[E : ClassTag, T : ClassTag](dataset: RDD[E], 
                       pickToSort: E => T,
                       rddSize: Option[Int] = None)
                       (implicit ev: T => Ordered[T]): RDD[E] = {
        
        import scala.math.log
        assert(numberOfMachines > 0, "number of machines must be non-zero!")
        
        val n = if (rddSize.isDefined) rddSize.get else dataset.count()
        if (n == 0) return dataset 
        
        val t = numberOfMachines
        val m = (n.toDouble / t).ceil
        
        def find_q_prob = {
            val q_prob: Double = (log(n*t).toDouble) / m
            q_prob
        }
        
        val q_prob = find_q_prob               
        val rnd = new scala.util.Random
        
        def probabilityMet(q: Double, rnd: scala.util.Random) = rnd.nextDouble() <= q

        sealed trait ShuffleData extends java.io.Serializable
        final case class Sample(val sample: List[T]) extends ShuffleData
        final case class MachineData(val data: List[E]) extends ShuffleData
        
        sealed case class Boundaries(val boundaries: Array[T]) extends java.io.Serializable


        def round1Map(arr: Array[E]): List[(MachineId, ShuffleData)] = {
            val sample = arr.filter(_ => probabilityMet(q_prob, rnd)).map(pickToSort)
            val sampleResult = for (i <- 0 to lastMachineId) yield (i, new Sample(sample.toList))
            val currMachineId = getMachineId()
            val mapResult = (currMachineId, new MachineData(arr.toList))::sampleResult.toList
            mapResult
        }
        

        def round1Reduce(keyIterable: (MachineId, Iterable[ShuffleData])): (Boundaries, MachineData) = {
            val (key, dataIterable) = keyIterable
            
            val (samples, machineDataArr) = dataIterable.partition(_.isInstanceOf[Sample])
            val MachineData(machineData) = machineDataArr.toList.head
            val samplesSorted = samples.flatMap(_ match { case Sample(sample) => sample; case _ => throw new Exception("wrong data in sample!")}).toArray.sorted
            val s = samplesSorted.size
            val step = (s.toDouble / t).ceil.toInt
            val boundaries = for (i <- 1 to t - 1) yield samplesSorted((i*step) min (s-1))
            
            (new Boundaries(boundaries.toArray), new MachineData(machineData))
        }

        def round2Map(boundariesData: (Boundaries, MachineData)): List[(MachineId, List[E])] = {
            val (Boundaries(sortedBoundaries), machineData) = boundariesData
            val boundaryElemToPartition = (0 to t-2).zip(sortedBoundaries).toMap
            val sortedMachineData = machineData.data.sortBy(pickToSort)
            val lastBoundIndex = sortedBoundaries.size-1
            
            
            def getNextBoundIdx(elem: E, i: Int): Int = {
                import scala.util.Try
                val next_i = Try(sortedBoundaries.zip(0 to t-2).filter{ case (b, i) => pickToSort(elem) <= b }.head._2).toOption.getOrElse(t-1)
                if (!(next_i > i)) throw new Exception("zepsuty index granpride")
                next_i
            }
            
            val add2map = (map: Map[Int, List[E]], elem: E, i: Int) => map + (i -> (elem::map(i)))
            val (_, splitedElemsByPartition) = sortedMachineData.
                                               foldLeft((0, Map[Int, List[E]]().withDefaultValue(List[E]())))
                                               { (acc, elem) => val (i, map) = (acc._1, acc._2)
                                                  if (i == lastBoundIndex+1) (i, add2map(map, elem, i))
                                                  else if (pickToSort(elem) <= sortedBoundaries(i)) (i, add2map(map, elem, i))
                                                  else {val next_i = getNextBoundIdx(elem, i); (next_i, add2map(map, elem, next_i)) }
                                               }
            
            for (i <- (0 to t-1).toList) yield (i, splitedElemsByPartition(i))
        }
        
        def round2Reduce(keyIterable: (MachineId, Iterable[List[E]])): List[E] = {
            val (key, dataIterable) = keyIterable
            dataIterable.flatMap(identity).toArray.sortBy(pickToSort).toList
        }
        
        val round1result = mapReduce(dataset.repartition(numberOfMachines).glom(), round1Map(_), round1Reduce(_))
        val round2result = mapReduce(round1result, round2Map(_), round2Reduce(_))
        val teraSortResult = round2result
        
        teraSortResult
    }
    
    
    def prefixSum[E : ClassTag, T : ClassTag, W](dataset: RDD[E],
                          pickToSort: E => T, 
                          weight: E => W, 
                          sum: (W, W) => W,
                          rddSize: Option[Int] = None)
                          (implicit ev: T => Ordered[T]): RDD[(E, W)] = {
                              
        assert(numberOfMachines > 0, "number of machines must be non-zero!")
        
        sealed trait ShuffleData extends java.io.Serializable
        final case class MachineSum(val sum: W) extends ShuffleData
        final case class MachineData(val data: List[E]) extends ShuffleData
        
        val n = if (rddSize.isDefined) rddSize.get else dataset.count()
        if (n == 0) return dataset.map(elem => (elem, weight(elem)))
        
        val t = numberOfMachines
        val m = (n.toDouble / t).ceil
        
        
        def roundMap(arr: Array[E]): List[(MachineId, ShuffleData)] = {
            import scala.util.Try
            val machineSum: Option[W] = Try(arr.map(elem => weight(elem)).reduce(sum)).toOption
            val currMachineId = getMachineId()
            lazy val machineSumShuffle = (for (i <- (currMachineId + 1 to t-1).toList) yield (i, MachineSum(machineSum.get)))
            val machineDataShuffle = (currMachineId, new MachineData(arr.toList))
            val mapResult = if (machineSum.isDefined) machineDataShuffle::machineSumShuffle else List(machineDataShuffle)
            mapResult
        }
        def roundReduce(keyIterable: (MachineId, Iterable[ShuffleData])): List[(E, W)] = {
            val (key, dataIterable) = keyIterable
            
            val (previousMachineSums, machineDataArr) = dataIterable.partition(_.isInstanceOf[MachineSum])
            val MachineData(machineData) = machineDataArr.toList.head
            
            lazy val totalSumPreviousMachines: W = (previousMachineSums: Iterable[ShuffleData]).map{ _ match { case MachineSum(sum) => sum
                                                                              case _ => throw new Exception("dane maszyny wsrod sum czesciowych")
                                                                            } }.reduce(sum): W

                                                      
            lazy val startWeight = weight(machineData.head)
            lazy val startAcc = (startWeight, List[W](startWeight))
            
            lazy val foldStep = (acc: (W, List[W]), elem: E) => {
                                val (prefixSum, l) = acc
                                val newPrefixSum: W = sum(prefixSum, weight(elem)): W
                                (newPrefixSum, newPrefixSum::l)
                            }
                                
            lazy val (_, prefixSumReversed) = machineData.tail.foldLeft(startAcc)(foldStep)
            lazy val prefixSum = prefixSumReversed.reverse
            
            val currMachineId = getMachineId()
            lazy val globalPrefixSum = if (currMachineId == firstMachineId) prefixSum 
                                  else prefixSum.map(sum(totalSumPreviousMachines, _))
                                  
            lazy val reduceResult = machineData.zip(globalPrefixSum)

            if (machineData.nonEmpty) reduceResult else Nil
        }
        
        val rddSorted = terasort[E, T](dataset, pickToSort, rddSize)
        val prefixSumResult = mapReduce(rddSorted.glom(), roundMap(_), roundReduce(_))

        prefixSumResult
    }
    
    
    def ranking[E : ClassTag, T : ClassTag](dataset: RDD[E], 
                      pickToSort: E => T,
                      rddSize: Option[Int] = None)
                      (implicit ev: T => Ordered[T]): RDD[(E, Rank)] = {
                                           
        prefixSum[E, T, Rank](dataset, pickToSort, _ => 1, _ + _, rddSize)
    }

    def perfectBalanceSort[E : ClassTag, T : ClassTag](dataset: RDD[E],
                                 pickToSort: E => T,
                                 rddSize: Option[Int] = None)
                                 (implicit ev: T => Ordered[T]): RDD[(E, Rank)] = {
        
        assert(numberOfMachines > 0, "number of machines must be non-zero!")

        val n = if (rddSize.isDefined) rddSize.get else dataset.count()
        if (n == 0) return dataset.map( e => (e, 0))
        
        val t = numberOfMachines
        val m = (n.toDouble / t).ceil.toInt
        
        def roundMap(rankingPair: (E, Rank)): (MachineId, (E, Rank)) = {
            val (elem, rank) = rankingPair
            val receiverPartitionId = (rank-1) / m
            (receiverPartitionId, (elem, rank))
        }
        
        def roundReduce(keyIterable: (MachineId, Iterable[(E, Rank)])): List[(E, Rank)] = {
            keyIterable._2.toList.sortBy{ case (elem, rank) => rank }
        }
        
        val rddRanking = ranking[E, T](dataset, pickToSort, rddSize)
        val prefixSumResult = mapReduce(rddRanking, roundMap(_), roundReduce(_))
        
        prefixSumResult
    }
    
    
    def slidingAggregation[E : ClassTag, T : ClassTag, W : ClassTag](dataset: RDD[E], 
                                    pickToSort: E => T, 
                                    windowSize: Int,
                                    weight: E => W,
                                    aggregate: List[W] => W,
                                    rddSize: Option[Int] = None)
                                    (implicit ev: T => Ordered[T]): RDD[(E, W)] = {
                                        
        val slidingAggregationManyRes = slidingAggregationMany[E, T, W](dataset, pickToSort, windowSize, List(weight), List(aggregate), rddSize)
        val result = slidingAggregationManyRes.map{ case (elem, arr_W) => (elem, arr_W.head)}
        
        result
    }

    def slidingAggregationMany[E : ClassTag, T : ClassTag, W : ClassTag](dataset: RDD[E], 
                                    pickToSort: E => T, 
                                    windowSize: Int,
                                    weights: List[E => W],
                                    aggregates: List[List[W] => W],
                                    rddSize: Option[Int] = None)
                                    (implicit ev: T => Ordered[T]): RDD[(E, Array[W])] = {
        
        assert(numberOfMachines > 0, "number of machines must be non-zero!")
        assert(weights.size == aggregates.size, "different number of weights and aggregates functions")
        
        val n = if (rddSize.isDefined) rddSize.get else dataset.count()
        if (n == 0) return dataset.map(e => (e, Array[W]()))
        
        val t = numberOfMachines
        val m = (n.toDouble / t).ceil.toInt
        
                                        
        sealed trait ShuffleData extends java.io.Serializable
        final case class MachineAggregation(val aggregatedFrom: Int, val partialResult: Array[W]) extends ShuffleData
        final case class RelevantToMachine(val dataWithRank: Set[(E, Int)]) extends ShuffleData
        final case class MachineData(val data: Array[(E, Int)]) extends ShuffleData
                                        
        
        def roundMap(arr: Array[(E, Int)]): List[(MachineId, ShuffleData)] = {
            lazy val currMachineId: Int = getMachineId()
            lazy val emptyShuffle = List[(MachineId, ShuffleData)]()
            
            lazy val dataWeights: List[Array[W]] = arr.map{ case (elem, rank) => (for (weight: (E => W) <- weights) yield weight(elem)).toArray }.toList
            lazy val machineAggregationRes = for ((aggregate, i) <- aggregates zip (0 to weights.size-1)) yield aggregate(dataWeights.map(_.apply(i)).toList)
            lazy val machineAggregated = MachineAggregation(currMachineId, machineAggregationRes.toArray)
            
            lazy val singleRelevantMachineId = currMachineId + 1
            lazy val relevantMachineId: Int = currMachineId + ((windowSize-1).toDouble / m).floor.toInt
            lazy val relevantMachineIdexes: Set[Int] = Set(relevantMachineId, (relevantMachineId+1) min (lastMachineId))


            lazy val localAggregationShuffle: Set[(Int, ShuffleData)] = (for (i <- 0 to lastMachineId) yield (i, machineAggregated)).toSet
            lazy val machineDataShuffle: Set[(Int, ShuffleData)] = Set((currMachineId, MachineData(arr)))
            lazy val relevantMachineShuffle: Set[(Int, ShuffleData)] = if (windowSize <= m && currMachineId < lastMachineId) Set((singleRelevantMachineId, RelevantToMachine(arr.toSet)))
                                                                  else if (windowSize > m && relevantMachineId <= lastMachineId) relevantMachineIdexes.map(i => (i, RelevantToMachine(arr.toSet)))
                                                                  else Set()
                
            lazy val shuffleData = machineDataShuffle union localAggregationShuffle.toSet union relevantMachineShuffle
            
            if (arr.nonEmpty) shuffleData.toList else emptyShuffle
        }

            def roundReduce(keyIterable: (MachineId, Iterable[ShuffleData])): List[(E, Array[W])] = {
            val (key, dataIterable) = keyIterable
            val currMachineId: Int = getMachineId()
            val emptyRes = List[(E, Array[W])]()
            
            import scala.util.Try

            val (machineAggregations, machineDataAndRelevantToMachine) = dataIterable.partition(_.isInstanceOf[MachineAggregation])
            val (relevantToMachine, machineDataArr) = machineDataAndRelevantToMachine.partition(_.isInstanceOf[RelevantToMachine])
            
            if (machineDataArr.isEmpty) return emptyRes
            
            
            val MachineData(machineData) = machineDataArr.head
            val dataWeights: Array[Array[W]] = machineData.map{ case (elem, rank) => (for (weight <- weights) yield weight(elem)).toArray }  

            val machineIdPartialAggregation: Map[MachineId, Array[W]] = machineAggregations.map{ _ match { case MachineAggregation(aggregatedFrom, partialResult) => (aggregatedFrom, partialResult); 
                                                                                                             case _ => throw new Exception("not MachineAggregation object!") }
                                                                                                 }.toMap
            
                                                                                      
            val relevant = relevantToMachine.map{ _ match { case RelevantToMachine(dataWithRank) => dataWithRank
                                                            case _ => throw new Exception("not RelevantToMachine object!") } }
                                                .reduceOption(_ union _)
                                                .getOrElse(Set())
                                                .toArray
                                                .sortBy{ case (_, rank) => rank }

            def calculateWindow(elemRank: Rank): Array[W] = {
                def windowStart(rank: Rank) = (rank - windowSize + 1)
                def rankToMachine(rank: Rank): Int = (windowStart(rank) / m.toDouble).ceil.toInt -1
                implicit def bool2int(b:Boolean) = if (b) 1 else 0
                
                val α: Int = rankToMachine(elemRank)
                lazy val leftMostRelMachine = rankToMachine(machineData.head._2)
                lazy val lastRelevantIdx = (relevant.size == 2) + (leftMostRelMachine < α && leftMostRelMachine >= 0)*m + (rankToMachine(elemRank) >= 0)*m

                def takeRelevant(elemRank: Rank): Array[Array[W]] = {
                    lazy val windowStartRank = (windowStart(elemRank)) max 1
                    lazy val firstRelevantMachineId = leftMostRelMachine
                    lazy val startRelevantIdx = (relevant.size == 2) + (leftMostRelMachine < α && firstRelevantMachineId >= 0)*m + ((windowStartRank-1) % m)
                    lazy val rel = relevant.slice(startRelevantIdx, lastRelevantIdx)
                    lazy val res = rel.map(elem => (for (weight <- weights) yield weight(elem)).toArray)
                    if (α < currMachineId) res else Array()
                }
                
                def takePartialAggregations(elemRank: Rank): Array[Array[W]] = {
                    (for (j <- ((α+1) max 0 to (currMachineId-1)).toList) yield machineIdPartialAggregation(j)).toArray
                }
                
                def takeOnThisMachine(elemRank: Rank): Array[Array[W]] = {
                    def rankToIndexesRange(rank: Rank): (Int, Int) = { 
                        val lastElemIdx = (rank-1) % m 
                        ((lastElemIdx - windowSize + 1), lastElemIdx) 
                    }
                    val (first, last) = rankToIndexesRange(elemRank)
                    dataWeights.slice(first, last+1)
                }
                
                lazy val w1 = takeRelevant(elemRank)
                lazy val w2 = takePartialAggregations(elemRank)
                lazy val w3 = takeOnThisMachine(elemRank)
                lazy val w1_w2_w3 = w1 ++ w2 ++ w3        


                lazy val windowRes = for ((aggregate, i) <- aggregates zip (0 to weights.size-1)) yield aggregate(w1_w2_w3.map(_.apply(i)).toList)
                
                windowRes.toArray
            }

            lazy val slidingAggregationRes = machineData.map{ case (elem, rank) => (elem, calculateWindow(rank)) }.toList
            slidingAggregationRes
        }
        
        val datasetPerfectSorted = perfectBalanceSort[E, T](dataset, pickToSort)
        val slidingAggregationResult = mapReduce(datasetPerfectSorted.glom(), roundMap(_), roundReduce(_))
        
        slidingAggregationResult
    }
}

