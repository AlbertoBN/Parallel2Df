using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Parallel2Df
{
    class Program
    {
        static void Main(string[] args)
        {
            /*
             * Suppose we have 2 nested Parallel.ForEach. The outer one ha parallelism of 1 and the inner of 10.
             * Both outer and inner are doing I/O ops. The outer from the network and the inner to a disk drive
             */
            List<string> strList = new List<string>();
            strList.Add("FirstSource");
            strList.Add("SecondSource");

            ParallelOptions netWorkOptions = new ParallelOptions();
            netWorkOptions.MaxDegreeOfParallelism = 1;

            ParallelOptions hardDriveOptions = new ParallelOptions();
            hardDriveOptions.MaxDegreeOfParallelism = 10;

            Parallel.ForEach(strList, netWorkOptions, (source) =>
            {
                byte[] buffer =  DownloadFromNetwork(source);

                List<Fragment> fragments = BreakToFragments(buffer);

                Parallel.ForEach(fragments, hardDriveOptions, (fragment) => 
                {

                    SaveFragment(fragment);

                });

            });
        }

        private static void SaveFragment(Fragment fragment)
        {
            Thread.Sleep(2500);
        }

        private static List<Fragment> BreakToFragments(byte[] buffer)
        {
            List<Fragment> fragments = new List<Fragment>();
            for(int i=0; i<1024;i++)
            {
                Thread.Sleep(10);
                Random rand = new Random(DateTime.Now.Millisecond);
                byte[] data = new byte[1024];
                rand.NextBytes(data);
                fragments.Add(new Fragment() { Offset = rand.Next(), Data = data, FileName = $"Lorem_{rand.Next()}" });
            }

            return fragments;
        }

        private static byte[] DownloadFromNetwork(string source)
        {
            byte[] buffer = new byte[1024];
            Random rand = new Random();
            rand.NextBytes(buffer);
            Thread.Sleep(2500);
            
            return buffer;
        }
    }
}
