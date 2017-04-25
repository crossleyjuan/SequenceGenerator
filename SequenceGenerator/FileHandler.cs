using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SequenceGenerator
{
    public class FileHandler
    {
        public class Data
        {
            public long value;
            public DateTime date;
        };

        #region Member Variables
        private string FileName { get; set; }
        private string FileLockName { get { return FileName + ".lock"; } }

        private Mutex _mutex;

        #endregion

        #region Singleton Elements
        private static Dictionary<string, FileHandler> _handlers = new Dictionary<string, FileHandler>();
        #endregion

        public static FileHandler GetHandler(string path, string fileName)
        {
            string fullFileName = Path.Combine(path, fileName + ".dat");
            FileHandler result = null;
            if (!_handlers.ContainsKey(fullFileName))
            {
                lock (_handlers)
                {
                    if (!_handlers.ContainsKey(fullFileName))
                    {
                        FileHandler handler = new FileHandler(fullFileName);
                        _handlers.Add(fullFileName, handler);
                        result = handler;
                    }
                    else
                    {
                        result = _handlers[fullFileName];
                    }
                }
            }
            else
            {
                result = _handlers[fullFileName];
            }
            return result;
        }

        private FileHandler(string fullFileName)
        {
            this.FileName = fullFileName;
            _mutex = new Mutex();
        }

        private void Lock()
        {
            _mutex.WaitOne(10000);
            if (File.Exists(this.FileLockName))
            {
                throw new ApplicationException("Lock timeout");
            }
            FileStream fsLock = new FileStream(FileLockName, FileMode.Create);
            fsLock.WriteByte(0);
            fsLock.Close();
            _mutex.ReleaseMutex();
        }

        private void Release()
        {
            _mutex.WaitOne(10000);
            File.Delete(FileLockName);
            _mutex.ReleaseMutex();
        }

        private void internalWrite(long value, DateTime date)
        {
            FileStream f = new FileStream(FileName, FileMode.OpenOrCreate);
            f.Seek(0, SeekOrigin.Begin);

            string sdate = string.Format("{0:yyyy}{0:MM}{0:dd}", date);
            byte[] bdate = ASCIIEncoding.ASCII.GetBytes(sdate);
            f.Write(bdate, 0, bdate.Length);

            byte[] data = ASCIIEncoding.ASCII.GetBytes(string.Format("{0}", value));
            f.Write(data, 0, data.Length);

            f.Close();
        }

        public void WriteSequenceValue(long value, DateTime date)
        {
            Lock();
            internalWrite(value, date);
            Release();
        }

        private Data internalRead()
        {
            FileStream f = new FileStream(FileName, FileMode.OpenOrCreate);
            f.Seek(0, SeekOrigin.Begin);
            byte[] data = new byte[1024];
            int readed = f.Read(data, 0, 1024);
            long lvalue = 0;
            Data result = null;
            if (readed > 0)
            {
                Data d = new Data();
                string date = ASCIIEncoding.ASCII.GetString(data, 0, 8);
                d.date = new DateTime(Convert.ToInt32(date.Substring(0, 4)), Convert.ToInt32(date.Substring(4, 2)), Convert.ToInt32(date.Substring(6, 2)));
                d.value = Convert.ToInt64(ASCIIEncoding.ASCII.GetString(data, 8, readed - 8));
                result = d;
            }
            f.Close();
            return result;
        }

        public Data ReadSequence()
        {
            Lock();
            Data data = internalRead();
            Release();
            return data;
        }

        public Data InitializeSequence(long value)
        {
            Data result = new Data()
            {
                value = value,
                date = DateTime.Now
            };
            internalWrite(result.value, result.date);
            return result;
        }

        public long IncrementSequence(int inc)
        {
//            Lock();
            Data data = internalRead();
            if (data == null)
            {
                data = InitializeSequence(inc);
            }
            else
            {
                data.value += inc;
                internalWrite(data.value, data.date);
            }
 //           Release();
            return data.value;
        }


    }
}
