using BizagiCL;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace TestApp
{
    class Program
    {
        [StructLayout(LayoutKind.Sequential)]
        public struct SYSTEMTIME
        {
            public short wYear;
            public short wMonth;
            public short wDayOfWeek;
            public short wDay;
            public short wHour;
            public short wMinute;
            public short wSecond;
            public short wMilliseconds;
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool SetSystemTime(ref SYSTEMTIME st);

        static void Main(string[] args)
        {
            TestSimple();
            TestFullLoad();
            //TestChangeDate();
            TestConcurrency();
        }

        private static void TestFullLoad()
        {
            long initial = KeyGen.GetKey("TestN");
            for (int x = 0; x < 1000; x++)
            {
                long test = KeyGen.GetKey("TestN");
                Debug.Assert(test == initial + x + 1);
            }
        }

        private static SYSTEMTIME GetDateST(DateTime date)
        {
            SYSTEMTIME result = new SYSTEMTIME {
                 wDay = (short)date.Day,
                 wDayOfWeek = (short)date.DayOfWeek,
                 wHour = (short)date.Hour,
                 wMilliseconds = (short)date.Millisecond,
                 wMinute = (short)date.Minute,
                 wMonth = (short)date.Month,
                 wSecond = (short)date.Second,
                 wYear = (short)date.Year
            };

            return result;
        }

        private static void TestChangeDate()
        {
            long val1 = KeyGen.GetKey("Test");
            long val2 = KeyGen.GetKey("TEST");
            Debug.Assert(val1 == (val2 - 1), "The val2 should be val1 + 1");

            SYSTEMTIME testDate = GetDateST(DateTime.Now.AddDays(1));

            SYSTEMTIME backupDate = GetDateST(DateTime.Now);
            SetSystemTime(ref testDate); // invoke this method.

            try
            {
                Thread.Sleep(2000);
                long val3 = KeyGen.GetKey("TEST");
                // This has to be a new value
                Debug.Assert(val3 == 1);
            }
            catch {}
            finally {
                SetSystemTime(ref backupDate); // invoke this method.
            }
            
        }

        private static void TestSimple()
        {
            long val1 = KeyGen.GetKey("Test");
            long val2 = KeyGen.GetKey("TEST");
            Debug.Assert(val1 == (val2 -1), "The val2 should be val1 + 1");
        }

        private static System.Collections.Generic.List<long> _previousIds = new List<long>();

        private const int IDS = 10000;
        private static void Execute()
        {
            List<long> ids = new List<long>();
            for (int x = 0; x < IDS; x++)
            {
                long id = KeyGen.GetKey("TESTX");
                if (id == 0)
                {
                    throw new ApplicationException("id is 0");
                }
                ids.Add(id);
            }
            lock (_previousIds)
            {
                _previousIds.AddRange(ids);
            }
        }

        class TempComparer : IComparer<long>
        {
            public int Compare(long x, long y)
            {
                if (x < y)
                {
                    return -1;
                }
                else if (x == y)
                {
                    return 0;
                }
                else
                {
                    return 1;
                }
            }
        }
        private static void TestConcurrency()
        {
            List<Thread> list = new List<Thread>();
            for (int x = 0; x < 100; x++) {
                Thread t = new Thread(Execute);
                list.Add(t);
                t.Start();
            }

            foreach (Thread t in list)
            {
                t.Join();
            };

            // Check for duplicates
            SortedSet<long> test = new SortedSet<long>(new TempComparer());
            for (var x = 0; x < _previousIds.Count; x++)
            {
                long l = _previousIds[x];
                if (l == 0)
                {
                    throw new ApplicationException(string.Format("{0} already existed.", l));
                }
                if (!test.Add(l))
                {
                    throw new ApplicationException(string.Format("{0} already existed.", l));
                }
            }
        }
    }
}
