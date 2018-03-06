using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace Parallel2Df
{
    class Program
    {
        static void Main(string[] args)
        {
            /*
             * Suppose we have 2 nested Parallel.ForEach. The outer one has parallelism of 1 and the inner of 10.
             * Both outer and inner are doing I/O ops. The outer from the network and the inner to a disk drive
             */

            var pipeline = CreatePipelineComponents();
            int i = 0;
            while (i!=10)//some condition
            {
                pipeline.Post(DownloadFromNetwork("FirstSource"));
                i++;
                Thread.Sleep(10);//Imagine some more processing here
            }

            Console.ReadKey();
            pipeline.Complete();

            Task.WaitAny(pipeline.Completion);

            Console.WriteLine("Complete");
        }

        private static BatchBlock<byte[]> CreatePipelineComponents()
        {
            var taskSchedulerPair = new ConcurrentExclusiveSchedulerPair();

            var dataPusher = new BatchBlock<byte[]>(5);

            //This one can run along other tasks. The transform block may transform several batches of fragments
            var fragmenter = new TransformBlock<byte[][], List<Fragment>>((buffers) => {

                //If we want to go wild we can set a Parallel.ForEach here and have each transform block spawn more tasks.
                //However the more the less merrier in this case so we do not go wild
                Console.WriteLine($"Entering fragmenter with {buffers.Length} buffers");

                List<Fragment> fragments = new List<Fragment>();
                foreach (var buffer in buffers)
                {
                    fragments.AddRange(BreakToFragments(buffer));
                }
                Console.WriteLine("Exiting fragmenter");
                return fragments;
            },
            new ExecutionDataflowBlockOptions
            {
                TaskScheduler = taskSchedulerPair.ConcurrentScheduler,
                MaxDegreeOfParallelism = Environment.ProcessorCount-1
            });

            var fragmentSaver = new ActionBlock<List<Fragment>>((fragments) => {

                //I want the action block in the pipeline to be exclusive but..
                //To be able to save 5 fragments in one sitting.
                //Nesting a DataFlow block would be extraneous.
                Console.WriteLine("Entering fragment SAVER");
                ParallelOptions options = new ParallelOptions();
                options.MaxDegreeOfParallelism = 5;
                ConcurrentBag<string> bag = new ConcurrentBag<string>();

                Parallel.ForEach(fragments, options, (fragment) => {
                    SaveFragment(fragment);
                    bag.Add(fragment.FileName);
                });

                fragments.Clear();

                Console.WriteLine("Exiting fragment SAVER");
            },
            new ExecutionDataflowBlockOptions
            {
                TaskScheduler = taskSchedulerPair.ExclusiveScheduler
            });

            dataPusher.LinkTo(fragmenter);
            fragmenter.LinkTo(fragmentSaver);

            return dataPusher;
        }
        private static void SaveFragment(Fragment fragment)
        {
            Console.WriteLine($"-- Saving fragment {fragment.FileName}");
            Thread.Sleep(2500);
        }

        private static List<Fragment> BreakToFragments(byte[] buffer)
        {
            List<Fragment> fragments = new List<Fragment>();
            for(int i=0; i<buffer.Length/10;i++)
            {
                Thread.Sleep(10);
                Random rand = new Random(DateTime.Now.Millisecond);
                byte[] data = new byte[buffer.Length / 10];
                rand.NextBytes(data);
                fragments.Add(new Fragment() { Offset = rand.Next(), Data = data, FileName = $"Lorem_{rand.Next()}" });
            }
            Console.WriteLine($"Created {fragments.Count} fragments");
            return fragments; 
        }

        private static byte[] DownloadFromNetwork(string source)
        {
            Console.WriteLine("Downloading from network");
            byte[] buffer = new byte[1024];
            Random rand = new Random();
            rand.NextBytes(buffer);
            Thread.Sleep(2500);
            
            return buffer;
        }
    }
}
