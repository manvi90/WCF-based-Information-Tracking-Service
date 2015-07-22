using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Diagnostics;
using Isis;

namespace IsisInformationTrackingService
{
    class Program
    {
        //handlers variable
        public const int SET = 0;
        public const int GET = 1;
        public const int calculateTime = 2;

        static int myRank = 0;
        private static View view = null;
        static List<string> attribute = new List<string>();

        //collection autoEntity
        static Dictionary<string, Dictionary<string, string>> autoEntity = new Dictionary<string, Dictionary<string, string>>();
        //collection totalTime:
        private static List<long> totalTime = new List<long>();

        //testing
        private static string[] entityName = { "car", "bike", "scooty", "motorcycle", "truck", "ship", "plane", "entity1", "entity2", "entity3", "entity4", "entity5", "entity6", "entity7", "entity8", "entity9", "entity10", "entity11", "entity12", "entity13", };
        private static string[] attributeName = { "company", "model", "price" };
        private static string[][] attributeValue = new string[3][] { new string[] { "honda", "ford", "maruti", "suzuki", "tvs", "yamaha", "bmw", "company1", "company2", "company3", "company4", "company5", "company6", "company7", "company8", "company9", "company10", "company11", "company12", "company13", }, new string[] { "model1", "model2", "model3", "model4", "model5", "model6", "model7", "model8", "model9", "model10", "model11", "model12", "model13", "model14", "model15", "model16", "model17", "model18", "model19", "model20", }, new string[] { "price1", "price2", "price3", "price4", "price5", "price6", "price7", "price8", "price9", "price10", "price11", "price12", "price13", "price14", "price15", "price16", "price17", "price18", "price19", "price20" } };

        private static void Dataset()
        {
            if (autoEntity.Count == 0)
            {

                for (int i = 0; i < entityName.Length; i++)
                {
                    autoEntity.Add(entityName[i], new Dictionary<string, string>());
                    for (int j = 0; j < attributeName.Length; j++)
                    {
                        autoEntity[entityName[i]].Add(attributeName[j], attributeValue[j][i]);
                    }

                }
            }

        }

        static void Main(string[] args)
        {
            IsisSystem.Start();
            Group g = new Group("automobile");

            Semaphore sem = new Semaphore(0, 20);
            Semaphore timeSem = new Semaphore(0, 1);

            //view handler
            g.ViewHandlers += (ViewHandler)delegate(View v)
            {
                view = v;
                Console.WriteLine("Auto View" + v);
                Console.Title = "View=" + v.viewid + "Rank=" + v.GetMyRank();
                myRank = v.GetMyRank();
                if (v.members.Length == 1)
                {
                    sem.Release();
                }

            };


            //SET handler
            g.Handlers[SET] += (Action<string, List<string>>)delegate(string entityName, List<string> attributes)
            {
                //               Console.WriteLine("entering into SET");
                lock (autoEntity)
                {

                    string[] attr;
                    if (!autoEntity.ContainsKey(entityName))
                    {
                        //                     Console.WriteLine("adding new entity in Dictionary");
                        autoEntity.Add(entityName, new Dictionary<string, string>());
                    }
                    // else
                    {

                        foreach (string s in attributes)
                        {
                            attr = s.Split(new char[] { '=' });

                            if (attr[1].Equals("null"))
                            {
                                //                           Console.WriteLine("Removing entries for attribute value=null");
                                autoEntity[entityName].Remove(attr[0]);
                            }
                            else if (!autoEntity[entityName].ContainsKey(attr[0]))
                            {
                                autoEntity[entityName].Add(attr[0], attr[1]);
                            }
                            else
                            {
                                autoEntity[entityName][attr[0]] = attr[1];
                            }
                        }

                    }

                }
            };

            //GET handler- for ordered query
            g.Handlers[GET] += (Action<string, List<string>>)delegate(string entityName, List<string> attributes)
            {
                List<KeyValuePair<string, string>> getList = new List<KeyValuePair<string, string>>();
                Console.WriteLine("entering into GET");
                if (autoEntity.ContainsKey(entityName))
                {
                    foreach (string a in attributes)
                    {
                        if (autoEntity[entityName].ContainsKey(a))
                        {
                            getList.Add(autoEntity[entityName].ElementAt(0));
                        }
                        else
                        {
                            Console.WriteLine("attribute value doesnt exist");
                        }

                    }

                }
                else
                {
                    Console.WriteLine("key- entity name does not exist");
                }
                g.Reply(getList);

            };

            //Make Checkpoint handler
            g.MakeChkpt += (Isis.ChkptMaker)delegate(View v)
            {
                if (autoEntity.Count != 0)
                {

                    Console.WriteLine("entering into Make CheckPoint");
                    List<KeyValuePair<string, List<KeyValuePair<string, string>>>> database = new List<KeyValuePair<string, List<KeyValuePair<string, string>>>>();

                    foreach (string key in autoEntity.Keys)
                    {
                        database.Add(new KeyValuePair<string, List<KeyValuePair<string, string>>>(key, autoEntity[key].ToList<KeyValuePair<string, string>>()));
                    }
                    g.SendChkpt(database);
                    g.EndOfChkpt();
                }
            };

            //Load checkpoint handler
            g.LoadChkpt += (Action<List<KeyValuePair<string, List<KeyValuePair<string, string>>>>>)delegate(List<KeyValuePair<string, List<KeyValuePair<string, string>>>> incoming)
            {
                Console.WriteLine("entering into load checkpoint");
                autoEntity = incoming.ToDictionary(pair => pair.Key, pair => pair.Value.ToDictionary(st => st.Key, st => st.Value));
            };

            g.Handlers[calculateTime] += (Action<long>)delegate(long time)
            {

                Console.WriteLine("entering into calculate time");

                totalTime.Add(time);

                if (totalTime.Count == view.members.Length)
                {
                    timeSem.Release();
                }
            };


            g.Join();
            Dataset();
            sem.WaitOne();

            //100 Independent SET Reuests
            #region test case1
            //Thread testthread = new Thread(delegate()
            //{
            //    Console.WriteLine("testing case 1");
            //    int counter = 100;
            //    List<long> average = new List<long>();
            //    while (counter > 0)
            //    {
            //        Stopwatch sw = new Stopwatch();
            //        sw.Start();

            //        g.OrderedSend(SET, entityName[myRank], new List<string>() { "company=company1", "model=model1", "price=price1" });
            //        sw.Stop();
            //        average.Add(sw.ElapsedTicks / (Stopwatch.Frequency / (1000L)));
            //        counter--;
            //        Thread.Sleep(500);
            //    }

            //    long time = (average.Sum());
            //    g.OrderedSend(calculateTime, time);
            //    Console.WriteLine("server set time (ms)" + time);

            //});
            //testthread.Start();
            #endregion


            //test case: for same entity modify attributes by different server threads
            #region test case2

            //Thread testThread2 = new Thread(delegate()
            //{
            //    Console.WriteLine("testing test case 2");
            //    int counter = 100;
            //    List<long> average = new List<long>();
            //    while (counter > 0)
            //    {
            //        Stopwatch sw = new Stopwatch();
            //        sw.Start();
            //        g.OrderedSend(SET, entityName[1], new List<string>() { "company=company1", "model=model1", "price=price1" });

            //        sw.Stop();
            //        average.Add(sw.ElapsedTicks / (Stopwatch.Frequency / (1000L)));
            //        counter--;
            //        Thread.Sleep(500);
            //    }

            //    long time = (average.Sum());
            //    Console.WriteLine("server set time (ms) " + time);
            //    g.OrderedSend(calculateTime, time);
            //});
            //testThread2.Start();
            #endregion


            #region test case3
            //Thread testThread3 = new Thread(delegate()
            //           {
            //               Console.WriteLine("Testing test case 3");
            //               int counter = 100;
            //               Random r = new Random();
            //               List<long> average = new List<long>();
            //               List<string> attributeList = new List<string>();
            //               while (counter > 0)
            //               {
            //                   int attr_count = r.Next(0, attributeName.Length);
            //                   for (int i = 0; i < attr_count; i++)
            //                   {
            //                       int rint = r.Next(0, attributeName.Length);
            //                       attributeList.Add(attributeName[rint]);
            //                   }
            //                   int rentity = r.Next(0, entityName.Length);
            //                   Stopwatch sw = new Stopwatch();
            //                   sw.Start();
            //                   Get(entityName[rentity], attributeList);
            //                   sw.Stop();
            //                   average.Add(sw.ElapsedTicks / (Stopwatch.Frequency / (1000L)));
            //                   counter--;
            //                   Thread.Sleep(500);
            //               }

            //               long time = average.Sum();
            //               Console.WriteLine("server set time(ms)" + time);
            //               g.OrderedSend(calculateTime, time);
            //           });
            //testThread3.Start();


            #endregion

            //Test case4,5,6,7,8: x SET and y Get (keep on changing the setCount and getCount. Tested for (SET=99,Get=1),(SET=1,Get=99),(SET=50,Get=50),(SET=75,Get=25),(SET=25,Get=75),
            #region test case4
            Thread testThread4 = new Thread(delegate()
            {
                Console.WriteLine("testing in test case");
                long time = 0;
                int count = 100;
                int totalSET = 0;
                int totalGet = 0;
                int setCount = 75;
                int getCount = 25;

                Random r = new Random();
                List<long> average = new List<long>();

                while (count > 0)
                {
                    int handler = r.Next(0, 100);
                    if (totalSET >= setCount)
                    {
                        handler = 2;
                    }
                    if (totalGet >= getCount)
                    {
                        handler = 1;
                    }
                    if (handler % 2 == 0)
                    {
                        List<string> attributeList = new List<string>();
                        int attr_count = r.Next(0, attributeName.Length);
                        for (int i = 0; i < attr_count; i++)
                        {
                            int rint = r.Next(0, attributeName.Length);
                            attributeList.Add(attributeName[rint]);
                        }
                        count--;
                        totalGet++;

                        Stopwatch sw = new Stopwatch();
                        sw.Start();
                        Get(entityName[r.Next(0, entityName.Length)], attributeList);
                        sw.Stop();
                        average.Add(sw.ElapsedTicks / (Stopwatch.Frequency / (1000L)));
                        Thread.Sleep(500);
                    }
                    else
                    {
                        List<string> attributeList = new List<string>();
                        int attr_count = r.Next(0, attributeName.Length);
                        for (int i = 0; i < attr_count; i++)
                        {
                            int rint = r.Next(0, attributeName.Length);
                            attributeList.Add(attributeName[rint] + "=" + "manvi");
                        }
                        count--;
                        totalSET++;
                        Stopwatch sw = new Stopwatch();
                        sw.Start();
                        g.OrderedSend(SET, entityName[r.Next(0, entityName.Length)], attributeList);
                        sw.Stop();
                        average.Add(sw.ElapsedTicks / (Stopwatch.Frequency / 1000L));
                        Thread.Sleep(500);
                    }
                }
                time = average.Sum();
                g.OrderedSend(calculateTime, time);
                
            });

            testThread4.Start();
            #endregion

            timeSem.WaitOne();
            Console.WriteLine("total time=" + (totalTime.Sum()) / totalTime.Count);
            IsisSystem.WaitForever();
        }

        //Get function: for local look up: entity name and arguements as parameter
        public static Dictionary<string, string> Get(string entity, List<string> attributeList)
        {
            lock (autoEntity)
            {
                Dictionary<string, string> val = new Dictionary<string, string>();
                if (autoEntity.ContainsKey(entity))
                {
                    foreach (string s in attributeList)
                    {
                        if (autoEntity[entity].ContainsKey(s))
                        {

#if DEBUG
                            Console.WriteLine("attribute value is " + autoEntity[entity][s]);
#endif
                            if (!val.ContainsKey(s))
                            {
                                val.Add(s, autoEntity[entity][s]);
                            }
                        }
                    }

                }
                return val;
            }
}

 public static Dictionary<string, string> GetAllAttributes(string entity)
        {
            lock (autoEntity)
            {
               // Dictionary<string, string> val = new Dictionary<string, string>();
                if (autoEntity.ContainsKey(entity))
                {
                    Console.WriteLine("entity exists");
                }
                return autoEntity[entity];
            }
        }
    }
}
