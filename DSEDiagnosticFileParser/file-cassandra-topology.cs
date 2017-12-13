using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    internal sealed class file_cassandra_topology : file_yaml
    {
        public file_cassandra_topology(CatagoryTypes catagory,
                                        IDirectoryPath diagnosticDirectory,
                                        IFilePath file,
                                        INode node,
                                        string defaultClusterName,
                                        string defaultDCName,
                                        Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {            
        }

        /*
            10.1.37.0=DC1_SPARK:RAC1
            10.1.46.110=DC1_SPARK:RAC1

            10.1.44.100=DC1:RAC1
            10.1.39.216=DC1:RAC1
            10.1.36.100=DC1:RAC1
            10.1.44.20=DC1:RAC1
            10.1.44.21=DC1:RAC1
            10.1.38.59=DC1:RAC1

            # Cassandra Node IP=Data Center:Rack

            # default for unknown nodes
            default=DC1:RAC1

            # Native IPv6 is supported, however you must escape the colon in the IPv6 Address
            # Also be sure to comment out JVM_OPTS="$JVM_OPTS -Djava.net.preferIPv4Stack=true"
            # in cassandra-env.sh
            fe80\:0\:0\:0\:202\:b3ff\:fe1e\:8329=DC1:RAC3

        */

        static bool ProcessedFile = false;

        protected override void ProcessPropertyValuePairs(IEnumerable<Tuple<string, string>> propvaluePairs)
        {
            base.ProcessPropertyValuePairs(propvaluePairs);

            if(!ProcessedFile)
            {
                string defaultDC = null;
                string defaultRack = null;

                foreach (var pair in propvaluePairs)
                {
                    var hostDCSplit = pair.Item1.Split('=');

                    if (hostDCSplit.Length == 2
                        && hostDCSplit[0].ToLower().Trim() != "unknown")
                    {
                        if (hostDCSplit[0].ToLower().Trim() != "default")
                        {
                            defaultDC = hostDCSplit[1].Trim();
                            if (pair.Item2.ToLower() != "unknown")
                            {
                                defaultRack = pair.Item2;
                            }
                        }
                        else
                        {
                            var node = Cluster.TryGetNode(hostDCSplit[0].Trim(), hostDCSplit[1].Trim(), this.Node.Cluster.Name);

                            if (node == null)
                            {
                                Logger.Instance.WarnFormat("{0}\t{1}\tSnitch defined Node \"{2}\" for DataCenter \"{3}\" for Rack \"{4}\" but wasn't found in current processing. This may indicated an error with the Snitch file or this processing.",
                                                                this.Node.Id,
                                                                this.File,
                                                                hostDCSplit[0],
                                                                hostDCSplit[1],
                                                                pair.Item2);
                            }
                            else
                            {
                                if (node.DataCenter == null)
                                {
                                    Cluster.AssociateDataCenterToNode(hostDCSplit[1].Trim(), node);
                                }

                                if (node.DSE.Rack == null && pair.Item2.ToLower() != "unknown")
                                {
                                    node.DSE.Rack = pair.Item2;
                                }
                            }
                        }
                    }
                }

                if(!string.IsNullOrEmpty(defaultDC))
                {
                    Cluster.GetNodes()
                        .Where(n => n.DataCenter == null)
                        .ForEach(n =>
                        {
                            Cluster.AssociateDataCenterToNode(defaultDC, n);

                            if(n.DSE.Rack == null && !string.IsNullOrEmpty(defaultRack))
                            {
                                n.DSE.Rack = defaultRack;
                            }
                        });
                }

                ProcessedFile = true;
            }
        }
    }
}
