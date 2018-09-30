using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace StarGroupByJoin
{
    class TestGround
    {

        //static void Main()
        //{
            //HashSet<Int64> hashSetA = new HashSet<Int64>();
            //HashSet<Int64> hashSetB = new HashSet<Int64>();
            //HashSet<Int64> hashSetC = new HashSet<Int64>();

            //var bigHashSetSize = 10000000;

            //// A and B 50% Intersection
            //// A and C 33% Intersection
            //for (int i = 0; i < bigHashSetSize; i++)
            //{
            //    hashSetA.Add(i);
            //    if (i % 2 == 0)
            //        hashSetB.Add(i);
            //    if (i % 3 == 0)
            //        hashSetC.Add(i);
            //}

            //var stopwatch1 = new Stopwatch();
            //stopwatch1.Start();
            //var intersectionSet1 = hashSetC.Intersect(hashSetA);
            //stopwatch1.Stop();

            //var stopwatch2 = new Stopwatch();
            //stopwatch2.Start();
            //var intersectionSet2 = hashSetB.Intersect(hashSetA);
            //stopwatch2.Stop();

            //Console.WriteLine("nbElemHashsetA=" + hashSetA.Count + "  nbElemHashsetB=" + hashSetB.Count + "  intersectionSize=" + intersectionSet1.Count() + ", " + intersectionSet2.Count());
            //Console.WriteLine("  Default Intersect: " + new TimeSpan(stopwatch1.ElapsedTicks).ToString());
            //Console.WriteLine("  My Intersect: " + new TimeSpan(stopwatch2.ElapsedTicks).ToString());
            //Console.WriteLine("  Gain : " + (int)(((float)stopwatch1.ElapsedTicks / stopwatch2.ElapsedTicks) * 100) + "%");

        //    parallelismTest();
        //    Console.ReadKey();


        //}
       

        public static void parallelismTest()
        {
            ParallelOptions po = new ParallelOptions { MaxDegreeOfParallelism = 2};
            int totalItems = 10000000;
            Random random = new Random();
            int[] randoms = new int[totalItems];
            int sum = 0;
            for (int i = 0; i < totalItems; i++)
            {
                randoms[i] = i;
            }
            var rangePartitioner = Partitioner.Create(randoms, EnumerablePartitionerOptions.NoBuffering);
            Stopwatch sw = Stopwatch.StartNew();
            Parallel.ForEach(rangePartitioner, po, (item) =>
            {
                Interlocked.Add(ref sum, item);
                Console.WriteLine("Item {0}, Thread {1}", item, Thread.CurrentThread.ManagedThreadId);
            });
            Console.WriteLine("Time taken: {0} ms.", sw.ElapsedMilliseconds);

        }

    }

}
