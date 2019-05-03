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
    class Program
    {
        static void Main(string[] args)
        {
            var node = new Uri("https://search-riptano-insights-stage-vmqawjqwydo4zerxyxu7burd4e.us-east-1.es.amazonaws.com");
            //var clusterId = new Guid("68d8d729-34ec-49c7-82d6-5b5c3e2bc242"); //DSE 6.0
            var clusterId = new Guid("8a165411-15c8-45b2-9cfd-119159b02384"); //DSE 5.1

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

            kss.First().ToString();

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
