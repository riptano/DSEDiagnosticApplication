using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Security.Cryptography.X509Certificates;
using Common;
using Nest;
using DSEDiagnosticLibrary;
using DSEDiagnosticInsightsES;
using DSEDiagnosticInsights;

namespace DSEDiagnosticInsightsConsole
{
    partial class Program
    {
        static void Main(string[] args)
        {
           //var node = new Uri("https://search-riptano-insights-stage-vmqawjqwydo4zerxyxu7burd4e.us-east-1.es.amazonaws.com");
            var node = new Uri("https://search-riptano-insights-test-vs27zxgfxtbfyhqzcpk3umt5g4.us-east-1.es.amazonaws.com");
            var clusterId = new Guid("cf8b173e-9e50-4d96-9c92-c0f15a233c63"); //DSE 5.1 AndersenHA51
            //var clusterId = "andersenha51";

            ESQuerySearch.OnException += (sender, eventArgs) =>
            {
                eventArgs.LogException = false;
                eventArgs.ThrowException = false;                
                //sender.Dump("Exception sender: ", 0);
                //eventArgs.Dump("Exception EventArgs: ", 0);
            };

            var esConnection = new Connection(node, new DateTimeOffsetRange(DateTimeOffset.Now, DateTimeOffset.Now - TimeSpan.FromDays(10)), clusterId);

            //esConnection.ESConnection.ServerCertificateDebugCallback += CheckCert;

            //esConnection.Dump(1);

            var dcs =  esConnection.BuildDCNodes(); //.Dump(1);
            var kss = esConnection.BuildSchema();

            var cancellationSource = new System.Threading.CancellationTokenSource();

            var datatableTasks = LoadDataTables(esConnection.Cluster, cancellationSource);
            var excelTasks = LoadExcelWorkbook(esConnection.Cluster, datatableTasks, cancellationSource);

            #region Wait for Tasks to Complete
            {
                var tasks = new List<Task>() { datatableTasks, excelTasks };
                //tasks.AddRange(aggInfoStatsTask);

                System.Threading.Tasks.Task.WaitAll(tasks.ToArray(), cancellationSource.Token);
            }
            #endregion

            // var tstKS = kss.FirstOrDefault(k => k.Name == "usprodda");
            // var tstKS1 = kss.FirstOrDefault(k => k.Name == "dse_insights");

            //esConnection.ESConnection.NetworkTrace.FirstOrDefault()?.Dump("Query: ", 0);
            //esConnection.ESConnection.NetworkTrace.Dump("Network Trace: ", 0);
        }

        public static bool CheckCert(object sender, X509Certificate cert, X509Chain chain, System.Net.Security.SslPolicyErrors errors)
        {
            //cert.Dump(0);
            //errors.Dump(0);

            return true;
        }
    }
}
