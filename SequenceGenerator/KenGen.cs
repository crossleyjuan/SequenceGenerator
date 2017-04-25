using SequenceGenerator;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Threading;

namespace BizagiCL
{
    /// <summary>
    /// Summary description for CKeyGenerator.
    /// </summary>
    public class KeyGen
    {
        #region Fields

        private Dictionary<string, KeyObject> m_odKeys = new Dictionary<string, KeyObject>(StringComparer.OrdinalIgnoreCase);
        private static int m_iSTACK_SIZE = 50;
        public static string GeneratorPath { get; set; }

        private static bool m_bHostUniqueSet;
        private static int m_iHostUnique;
        private static short m_iLastCount = -32768;
        private static long m_iLastTime = DateTime.Now.Ticks;

        private static int m_iUnique;
        private static long m_iTime;
        private static short m_iCount;

        #endregion

        #region Thread synchronization

        /// <summary>
        /// Read/Write Lock synchronization
        /// </summary>
        private static ReaderWriterLock m_oRWLock = new ReaderWriterLock();

        #endregion

        #region Singleton

        private static KeyGen m_oInstance = CreateInstance();

        private static KeyGen CreateInstance()
        {
            m_oInstance = new KeyGen();
            return m_oInstance;
        }

        static KeyGen()
        {
            GeneratorPath = ConfigurationManager.AppSettings["SEQGENERATORPATH"];
            if (string.IsNullOrEmpty(GeneratorPath))
            {
                GeneratorPath = Path.GetTempPath();
            }
        }

        /// <summary>
        /// For testing purposes only
        /// </summary>
        /// <returns></returns>
        //public static CKeyGenerator CreateTestingInstance()
        //{
        //    m_oInstance = new CKeyGenerator();
        //    if (Vision.Settings.CSettingsFactory.Settings["ID_STACK_SIZE"] == null)
        //    {
        //        m_iSTACK_SIZE = 50;
        //    }
        //    else
        //    {
        //        m_iSTACK_SIZE = Convert.ToInt32(Vision.Settings.CSettingsFactory.Settings["ID_STACK_SIZE"]);
        //    }
        //    return m_oInstance;
        //}

        private KeyGen()
        {
        }
        
        #endregion

        private static readonly object m_oToLock = new object();

        /**
         * Returns a new key for the desired generator name
         * Example:
         *      int id1 = BAKeyGenerator.getKey("MYTABLE1");
         *      int id2 = BAKeyGenerator.getKey("MYTABLE2");
         *
         * @param generatorName Name of the Generator
         * @return new Key
         */
        public static long GetKey(string generatorName)
        {
            int dotIndex = generatorName.LastIndexOf("."); 
            if (dotIndex > -1)
                generatorName = generatorName.Substring(dotIndex + 1);

            return m_oInstance.GetNextId(generatorName);
        }

        /// <summary>
        /// For Testing purposes only
        /// </summary>
        /// <param name="generatorName"></param>
        /// <returns></returns>
        //public int GetTestingKey(string generatorName)
        //{
        //    if (generatorName.IndexOf(".") > -1)
        //    {
        //        generatorName = generatorName.Substring(generatorName.LastIndexOf(".") + 1);
        //    }
        //    return this.GetNextId(generatorName);
        //}

        /**
         * Creates or updates a key in the Generator Table using the provided
         * generatorName
         * @param generatorName Name of the generator to be created or updated
         * @return a new int seed
         */
        private static long CreateKey(string sGeneratorName)
        {
            string _sGeneratorNameUpp = sGeneratorName.ToUpper();

            try
            {

                FileHandler handler = FileHandler.GetHandler(GeneratorPath, sGeneratorName);

                long _iIdResp = handler.IncrementSequence(m_iSTACK_SIZE);

                _iIdResp -= m_iSTACK_SIZE;

                long result = _iIdResp + 1;
                return result;
            }
            catch (Exception ex)
            {
                // Rethrow exception
                throw;
            }
        }

        /**
         * Returns a new key for the desired generator name
         * Example:
         *      int id1 = getNextId("MYTABLE1");
         *      int id2 = getNextId("MYTABLE2");
         *
         * @param generatorName Name of the Generator
         * @return new Key
         */
        private long GetNextId(string sGeneratorName)
        {
            KeyObject _oIdObj;
                
            // Simple check in the cache
            if (!m_odKeys.TryGetValue(sGeneratorName, out _oIdObj))
            {
                try
                {
                    m_oRWLock.AcquireWriterLock(10000);
                    // Double check in the cache to make it thread safe
                    if (!m_odKeys.TryGetValue(sGeneratorName, out _oIdObj))
                    {
                        #region Create the key
                        // The key does not exist in the cache, 
                        // then we need to create the key and start the object
                        int iNumRetries = 0;
                        bool bKeyGenerated = false;

                        // Attempt to create the key object mas 100 times
                        while (!bKeyGenerated)
                        {
                            try
                            {
                                // Generate the key and add it in the cache
                                long lId = CreateKey(sGeneratorName);
                                // Create object with cached metadata
                                _oIdObj = new KeyObject(sGeneratorName, lId);

                                m_odKeys.Add(sGeneratorName, _oIdObj);

                                // Set flag to true in order to end the loop
                                bKeyGenerated = true;
                            }
                            catch (Exception e)
                            {
                                // When an error is generated, internally the connection restarts, so the next request
                                // will get a new connection so we can retry again
                                iNumRetries++;
                                bKeyGenerated = false;

                                // Retry max 100 times, if it still has db connection failures
                                // must end loop and throw exception to outer modules
                                if (iNumRetries >= 100)
                                {
                                    throw;
                                }

                                // Sleep 50 milliseconds the thread ... to let the db server do other stuff
                                Thread.Sleep(50);
                            }
                        }
                        #endregion

                    }
                }
                finally
                {
                    m_oRWLock.ReleaseWriterLock();
                }
            }
            return _oIdObj.NextId();
        }

        /**
         * This class contains the pair key for a table, it used to get the ids
         * and also controls the STACK of numbers.
         */
        private class KeyObject
        {
            #region Fields

            private long m_iId;
            private int m_iUpdates;
            private string m_sGeneratorName;
            private int _iDate; // Date Owner so this can change everyday

            #region Table Metadata

            #endregion

            #endregion

            #region Constructor

            public static int GetCurrentDate()
            {
                DateTime now = DateTime.Now;
                int date = (now.Year * 10000) + (now.Month * 100) + (now.Day);
                return date;
            }

            public KeyObject(
                string generatorName, 
                long id
                )
            {
                m_iId = id;
                m_sGeneratorName = generatorName;
                m_iUpdates = 0;
                _iDate = GetCurrentDate();
            }

            #endregion

            #region Methods

            /**
             * Returns the next id for the current KeyObject, if the stack limit
             * is reached then it will get a new key seed.
             * @return a new id
             */
            public long NextId()
            {
                long lResp = -1;
                lock (this)
                {
                    #region Generate Next id
                    int iNumRetries = 0;
                    bool bKeyGenerated = false;
                    // If it's a new day then forces the new id from the db
                    if (GetCurrentDate() != _iDate)
                    {
                        m_iId = -1;
                    }
                    while (!bKeyGenerated)
                    {
                        try
                        {
                            // Special case when the key has lost synchronization with DB, 
                            // The id member is marked with -1 which forces to get key from DB
                            if (m_iId == -1)
                            {
                                m_iId = InitializeKey();
                            }

                            // Update counters
                            lResp = m_iId;
                            m_iId++;
                            m_iUpdates++;

                            // When the reserved ids has been used, then asks for more in the database
                            if (m_iUpdates >= m_iSTACK_SIZE)
                            {
                                AttemptToCreateKey();
                            }

                            // if no error occurred until here, then the key has been generated successfully
                            bKeyGenerated = true;
                        }
                        catch (Exception e)
                        {
                            // When an error is generated, internally the connection restarts, so the next request
                            // will get a new connection so we can retry again
                            iNumRetries ++;
                            bKeyGenerated = false;
                            
                            // Retry max 100 times, if it still has db connection failures
                            // must end loop and throw exception to outer modules
                            if (iNumRetries >= 100)
                            {
                                throw;
                            }

                            // Sleep 50 milliseconds the thread ... to let the db server do other stuff
                            Thread.Sleep(50);
                        }
                    }
                    #endregion
                }
                return lResp;
            }

            private long InitializeKey()
            {
                string _sGeneratorNameUpp = m_sGeneratorName.ToUpper();

                try
                {

                    FileHandler handler = FileHandler.GetHandler(GeneratorPath, m_sGeneratorName);

                    handler.InitializeSequence(m_iSTACK_SIZE);

                    return 1;
                }
                catch (Exception ex)
                {
                    // Rethrow exception
                    throw;
                }
            }

            /// <summary>
            /// Safely Creates a key managing any exception thrown
            /// If an exception is thrown set id member to -1, to indicates that needs synchronization, 
            /// and re-throw the exception
            /// </summary>
            private void AttemptToCreateKey()
            {
                m_iUpdates = 0;
                try
                {
                    m_iId = CreateKey(m_sGeneratorName);
                }
                catch
                {
                    m_iId = -1;
                    throw;
                }
            }

            #endregion

        }

    }

}
