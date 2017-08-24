using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DSEDiagnosticConsoleApplication
{
    public sealed class GCMonitor
    {
        static public readonly log4net.ILog LogHelper = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);

        private static volatile GCMonitor instance;
        private static object syncRoot = new object();

        private Thread gcMonitorThread;
        private ThreadStart gcMonitorThreadStart;

        private bool isRunning;
        private Common.ConsoleWriter consolerWriter = ConsoleDisplay.Console;
        private int nbrGCs = 0;

        public static GCMonitor GetInstance()
        {
            if (instance == null)
            {
                lock (syncRoot)
                {
                    instance = new GCMonitor();
                }
            }

            return instance;
        }

        private GCMonitor()
        {
            isRunning = false;
#if !MONO
            gcMonitorThreadStart = new ThreadStart(DoGCMonitoring);
            gcMonitorThread = new Thread(gcMonitorThreadStart);
            this.consolerWriter.ReserveRwWriteConsoleSpace("GCMonitor", 2, -1);
            this.consolerWriter.ReWrite("GCMonitor",
                                            "GC Notification Registered for {0} in Mode {1}",
                                            System.Runtime.GCSettings.IsServerGC ? "Server" : "Workstation",
                                            System.Runtime.GCSettings.LatencyMode);
            LogHelper.InfoFormat("GC Notification Registered for {0} in Mode {1}",
                                    System.Runtime.GCSettings.IsServerGC ? "Server" : "Workstation",
                                    System.Runtime.GCSettings.LatencyMode);
#endif
        }

        public void StartGCMonitoring()
        {
#if !MONO
            if (!isRunning)
            {
                GC.RegisterForFullGCNotification(10, 10);
                isRunning = true;
                gcMonitorThread.Start();
            }
#endif
        }

        public void StopGCMonitoring()
        {
            if (isRunning)
            {
                isRunning = false;
#if !MONO
                GC.CancelFullGCNotification();
                if (!gcMonitorThread.Join(1000))
                {
                    gcMonitorThread.Abort();
                }
#endif
            }
        }

        private void DoGCMonitoring()
        {
            long beforeGC = 0;
            long afterGC = 0;
            DateTime gcStart;

            try
            {

                while (isRunning)
                {
                    gcStart = DateTime.MinValue;

                    // Check for a notification of an approaching collection.
                    GCNotificationStatus s = GC.WaitForFullGCApproach(10000);
                    if (s == GCNotificationStatus.Succeeded)
                    {
                        //Call event
                        beforeGC = GC.GetTotalMemory(false);

                        var msg = string.Format("Full GC is imminent. Memory before GC: {0:###,###,##0} Nbr: {1}", beforeGC, ++this.nbrGCs);
                        this.consolerWriter.ReWrite("GCMonitor", msg);
                        gcStart = DateTime.Now;

                        GC.Collect();

                    }
                    else if (s == GCNotificationStatus.Canceled)
                    {
                        break;
                    }
                    else if (s == GCNotificationStatus.Timeout)
                    {
                        continue;
                    }
                    else if (s == GCNotificationStatus.NotApplicable)
                    {
                        continue;
                    }
                    else if (s == GCNotificationStatus.Failed)
                    {
                        continue;
                    }

                    if (gcStart == DateTime.MinValue)
                    {
                        gcStart = DateTime.Now;
                    }

                    while (isRunning)
                    {
                        var msg = string.Format(@"Running Full GC. Memory at GC: {0:###,###,##0} Nbr: {1} Started: {2:HH\:mm\:ss}", beforeGC, this.nbrGCs, gcStart);
                        this.consolerWriter.ReWrite("GCMonitor", msg);

                        // Check for a notification of a completed collection.
                        s = GC.WaitForFullGCComplete(100000);
                        if (s == GCNotificationStatus.Succeeded)
                        {
                            var duration = DateTime.Now - gcStart;

                            //Call event
                            afterGC = GC.GetTotalMemory(false);

                            msg = string.Format("GC has ended. Memory after GC: {0:###,###,##0} Nbr: {1} Duration: {2}", afterGC, this.nbrGCs, duration);
                            this.consolerWriter.ReWrite("GCMonitor", msg);

                            long diff = beforeGC - afterGC;

                            if (diff > 0)
                            {
                                var msg1 = string.Format("GC has ended. Collected memory: {0:###,###,##0} New Size: {1:###,###,##0} Nbr: {2} Duration: {3}", diff, afterGC, this.nbrGCs, duration);
                                this.consolerWriter.ReWrite("GCMonitor", msg1);
                            }

                            if (duration.TotalSeconds >= 5)
                            {
                                LogHelper.InfoFormat("GC Pause for {0:###,###,##0} seconds. Collected memory: {1:###,###,##0} New Size: {2:###,###,##0} Nbr: {3}",
                                                        duration.TotalSeconds,
                                                        diff,
                                                        afterGC,
                                                        this.nbrGCs);
                            }

                            break;
                        }
                        else if (s == GCNotificationStatus.Canceled)
                        {
                            break;
                        }
                        else if (s == GCNotificationStatus.Timeout)
                        {
                            continue;
                        }
                        else if (s == GCNotificationStatus.NotApplicable)
                        {
                            break;
                        }
                        else if (s == GCNotificationStatus.Failed)
                        {
                            break;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                LogHelper.Error("===> GC Error <===", e);
                this.consolerWriter.WriteLine("GC Error! See Log...");
            }
        }
    }
}
