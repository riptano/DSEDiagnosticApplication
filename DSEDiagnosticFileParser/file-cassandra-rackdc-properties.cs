using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    internal sealed class file_cassandra_rackdc_properties : file_DSE_env
    {
        public file_cassandra_rackdc_properties(CatagoryTypes catagory,
                                                IDirectoryPath diagnosticDirectory,
                                                IFilePath file,
                                                INode node,
                                                string defaultClusterName,
                                                string defaultDCName)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName)
        { }

        protected override void SetNodeAttribuesFromConfig(Tuple<string, string> propvaluePair)
        {
            /*
                dc=DC1
                rack = RAC1

                # Add a suffix to a datacenter name. Used by the Ec2Snitch and Ec2MultiRegionSnitch
                # to append a string to the EC2 region name.
                #dc_suffix=

                # Uncomment the following line to make this snitch prefer the internal ip when possible, as the Ec2MultiRegionSnitch does.
                # prefer_local=true
            */
            switch (propvaluePair.Item1)
            {
                case "dc":
                    if (this.Node.DataCenter == null)
                    {
                        Cluster.AssociateDataCenterToNode(propvaluePair.Item2, this.Node);                        
                    }
                    break;
                case "rack":
                    if (string.IsNullOrEmpty(this.Node.DSE.Rack))
                    {
                        this.Node.DSE.Rack = propvaluePair.Item2;
                    }
                    break;
                case "dc_suffix":
                    this.Node.DSE.DataCenterSuffix = propvaluePair.Item2;
                    break;
                default:
                    break;
            }
        }
    }
}
