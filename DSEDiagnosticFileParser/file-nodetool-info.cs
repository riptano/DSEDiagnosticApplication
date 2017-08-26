using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Common;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    class file_nodetool_info : DiagnosticFile
    {
        public file_nodetool_info(CatagoryTypes catagory,
                                    IDirectoryPath diagnosticDirectory,
                                    IFilePath file,
                                    INode node,
                                    string defaultClusterName,
                                    string defaultDCName,
                                    Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
        }

        public override IResult GetResult()
        {
            return new EmptyResult(this.File, null, null, this.Node);
        }

        public override uint ProcessFile()
        {
            var fileLines = this.File.ReadAllLines();
            uint nbrGenerated = 0;

            /*
            ID                     : c7e1ddc3-565f-40ed-91c1-320d729b25bd
            Gossip active          : true
            Thrift active          : true
            Native Transport active: true
            Load                   : 24.23 GB
            Generation No          : 1483630014
            Uptime (seconds)       : 59735
            Heap Memory (MB)       : 4697.67 / 8192.00
            Off Heap Memory (MB)   : 225.46
            Data Center            : Ejby
            Rack                   : RAC1
            Exceptions             : 0
            Key Cache              : entries 1190551, size 90.98 MB, capacity 100 MB, 878433 hits, 5987542 requests, 0.147 recent hit rate, 14400 save period in seconds
            Row Cache              : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
            Counter Cache          : entries 0, size 0 bytes, capacity 50 MB, 0 hits, 0 requests, NaN recent hit rate, 7200 save period in seconds
            Token                  : (invoke with -T/--tokens to see all 256 tokens)
            */

            string line;
            string lineCommand;
            string lineValue;
            int delimitorPos;

            foreach (var element in fileLines)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                ++this.NbrItemsParsed;
                line = element.Trim();

                if (string.Empty == line)
                {
                    continue;
                }

                delimitorPos = line.IndexOf(':');

                if (delimitorPos <= 0)
                {
                    continue;
                }

                lineCommand = line.Substring(0, delimitorPos).Trim().ToLower();
                lineValue = line.Substring(delimitorPos + 1).Trim();

                switch (lineCommand)
                {
                    case "gossip active":
                        this.Node.DSE.GossipEnabled =  bool.Parse(lineValue);
                        break;
                    case "thrift active":
                        this.Node.DSE.ThriftEnabled = bool.Parse(lineValue);
                        break;
                    case "native transport active":
                        this.Node.DSE.NativeTransportEnabled = bool.Parse(lineValue);
                        break;
                    case "load":
                        this.Node.DSE.StorageUsed = UnitOfMeasure.Create(lineValue, UnitOfMeasure.Types.Storage);
                        break;
                    case "generation no":
                        //dataRow["Number of Restarts"] = int.Parse(lineValue);
                        break;
                    case "uptime (seconds)":
                        this.Node.DSE.Uptime = UnitOfMeasure.Create(lineValue, UnitOfMeasure.Types.Time | UnitOfMeasure.Types.SEC);
                        break;
                    case "heap memory (mb)":
                        {
                            var splitHeap = lineValue.Split('/');
                            this.Node.DSE.Heap = UnitOfMeasure.Create(splitHeap[1].Trim(), UnitOfMeasure.Types.MiB | UnitOfMeasure.Types.Memory);
                            this.Node.DSE.HeapUsed = UnitOfMeasure.Create(splitHeap[0].Trim(), UnitOfMeasure.Types.MiB | UnitOfMeasure.Types.Memory);
                        }
                        break;
                    case "off heap memory (mb)":
                        this.Node.DSE.OffHeap = UnitOfMeasure.Create(lineValue, UnitOfMeasure.Types.MiB | UnitOfMeasure.Types.Memory);
                        break;
                    case "id":
                        if(this.Node.DSE.HostId == null)
                        {
                            ((Node)this.Node).DSE.HostId = new Guid(lineValue);
                        }
                        break;
                    case "token":
                        break;
                    case "datacenter":
                    case "data center":
                        if (this.Node.DataCenter == null)
                        {
                            if(Cluster.AssociateDataCenterToNode(lineValue, this.Node) != null)
                            {
                                ++nbrGenerated;
                            }
                        }
                        break;
                    case "rack":
                        if(string.IsNullOrEmpty(this.Node.DSE.Rack))
                        {
                            ((Node)this.Node).DSE.Rack = lineValue;
                        }
                        break;
                    case "exceptions":
                        this.Node.DSE.NbrExceptions = uint.Parse(lineValue);
                        break;
                    case "key cache":
                        this.Node.DSE.KeyCacheInformation = lineValue;
                        break;
                    case "row cache":
                        this.Node.DSE.RowCacheInformation = lineValue;
                        break;
                    case "counter cache":
                        this.Node.DSE.CounterCacheInformation = lineValue;
                        break;
                    default:
                        Logger.Instance.ErrorFormat("FileMapper<{3}>\t{0}\t{1}\tInvalid Line \"{2}\" found in NodeTool Info File.",
                                                        this.Node.Id,
                                                        this.File,
                                                        line,
                                                        this.MapperId);
                        break;
                }
            }

            this.Processed = true;
            return nbrGenerated;
        }
    }
}
