﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
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
            strList.Add("Source1");
            strList.Add("Source2");
            strList.Add("Source3");
            strList.Add("Source4");
            strList.Add("Source5");
            strList.Add("Source6");
            strList.Add("Source7");
            strList.Add("Source8");
            strList.Add("Source9");
            strList.Add("Source10");

            ParallelOptions netWorkOptions = new ParallelOptions();
            netWorkOptions.MaxDegreeOfParallelism = Environment.ProcessorCount-1;

            ParallelOptions hardDriveOptions = new ParallelOptions();
            hardDriveOptions.MaxDegreeOfParallelism = 5;
            Stopwatch sw = new Stopwatch();
            sw.Start();
            Parallel.ForEach(strList, netWorkOptions, (source) =>
            {
                byte[] buffer =  DownloadFromNetwork(source);

                List<Fragment> fragments = BreakToFragments(buffer);

                Parallel.ForEach(fragments, hardDriveOptions, (fragment) => 
                {

                    SaveFragment(fragment);

                });

            });

            sw.Stop();
            Console.WriteLine(sw.ElapsedMilliseconds);
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
