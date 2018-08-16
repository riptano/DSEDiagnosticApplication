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
    public class file_nodetool_info : DiagnosticFile
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


            ID                     : 8c512ce5-8cdd-4047-b327-0e8e4d4658f9
            Gossip active          : true
            Thrift active          : true
            Native Transport active: true
            Load                   : 702.19 GB
            Generation No          : 1503340279
            Uptime (seconds)       : 1371412
            Heap Memory (MB)       : 11351.44 / 16384.00
            Off Heap Memory (MB)   : 591.28
            Data Center            : EP1
            Rack                   : RAC1
            Exceptions             : 0
            Key Cache              : entries 185238, size 97.91 MB, capacity 100 MB, 2663650 hits, 4067944 requests, 0.655 recent hit rate, 14400 save period in seconds
            Row Cache              : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
            Counter Cache          : entries 0, size 0 bytes, capacity 50 MB, 0 hits, 0 requests, NaN recent hit rate, 7200 save period in seconds
            Percent Repaired       : 0.002445829365017248%
            Token                  : (invoke with -T/--tokens to see all 256 tokens)

            ID                     : df93723c-0714-4872-a8a2-e327ed2365ad
            Gossip active          : true
            Thrift active          : true
            Native Transport active: true
            Load                   : 438.5 GiB
            Generation No          : 1520869789
            Uptime (seconds)       : 610305
            Heap Memory (MB)       : 25275.49 / 32768.00
            Off Heap Memory (MB)   : 1384.04
            Data Center            : PIAD3N
            Rack                   : RACK01
            Exceptions             : 1
            Key Cache              : entries 152564, size 99.49 MiB, capacity 100 MiB, 285238161 hits, 291943238 requests, 0.977 recent hit rate, 14400 save period in seconds
            Row Cache              : entries 0, size 0 bytes, capacity 0 bytes, 0 hits, 0 requests, NaN recent hit rate, 0 save period in seconds
            Counter Cache          : entries 285666, size 50 MiB, capacity 50 MiB, 27356 hits, 150523 requests, 0.182 recent hit rate, 7200 save period in seconds
            Chunk Cache            : entries 7680, size 480 MiB, capacity 480 MiB, 90542270 misses, 750295628 requests, 0.879 recent hit rate, 183.301 microseconds miss latency
            Percent Repaired       : 4.996919574020069%
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
                        this.Node.DSE.GossipEnabled = bool.Parse(lineValue);
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
                        if (this.Node.DSE.HostId == null)
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
                            if (Cluster.AssociateDataCenterToNode(lineValue, this.Node) != null)
                            {
                                ++nbrGenerated;
                            }
                        }
                        break;
                    case "rack":
                        if (string.IsNullOrEmpty(this.Node.DSE.Rack))
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
                    case "chunk cache":
                        this.Node.DSE.ChunkCacheInformation = lineValue;
                        break;
                    case "percent repaired":
                        this.Node.DSE.RepairedPercent = UnitOfMeasure.Create(lineValue, UnitOfMeasure.Types.Percent);
                        break;
                    default:
                        Logger.Instance.ErrorFormat("FileMapper<{3}>\t{0}\t{1}\tInvalid Line \"{2}\" found in NodeTool Info File.",
                                                        this.Node.Id,
                                                        this.ShortFilePath,
                                                        line,
                                                        this.MapperId);
                        ++this.NbrErrors;
                        break;
                }
            }

            DateTimeOffset? captureDateTime = DetermineNodeCaptureDateTime(this.Node, this.ShortFilePath, this.DiagnosticDirectory);

            if (captureDateTime.HasValue)
            {
                this.Node.DSE.CaptureTimestamp = captureDateTime;

                if (Logger.Instance.IsDebugEnabled)
                {
                    Logger.Instance.InfoFormat("Node \"{0}\" using a Capture Date of {1}",
                                                this.Node,
                                                this.Node.DSE.CaptureTimestamp.HasValue
                                                    ? this.Node.DSE.CaptureTimestamp.Value.ToString(@"yyyy-MM-dd HH:mm:ss zzz")
                                                    : "<Unkown>");
                }
            }

            this.Node.UpdateDSENodeToolDateRange();

            this.Processed = true;
            return nbrGenerated;
        }

        public static DateTimeOffset? DetermineNodeCaptureDateTime(INode node, IFilePath filePath, IDirectoryPath sourceDir)
        {
            DateTimeOffset? captureDateTime = null;

            if (LibrarySettings.CaptureTimeFrameMatches != null
                    && !node.DSE.CaptureTimestamp.HasValue)
            {
                var strFile = filePath.PathResolved;

                if (sourceDir != null)
                {
                    var strDir = sourceDir.PathResolved;
                    int lastPos = 0;

                    for(; lastPos < strDir.Length; ++lastPos)
                    {
                        if(strDir[lastPos] != strFile[lastPos]) break;
                    }

                    if(lastPos > 0)
                    {
                        strFile = strFile.Substring(lastPos);
                    }
                }

                try
                {
                    var directories = strFile.Split(System.IO.Path.DirectorySeparatorChar);
                    
                    for (int nIdx = directories.Length - 1; nIdx >= 0; --nIdx)
                    {
                        var dirItem = directories[nIdx];
                        var regexMatch = LibrarySettings.CaptureTimeFrameMatches.Match(dirItem);

                        if (regexMatch.Success)
                        {
                            var year = regexMatch.Groups["year"].Value;
                            var month = regexMatch.Groups["month"].Value;
                            var day = regexMatch.Groups["day"].Value;
                            var hour = regexMatch.Groups["hour"].Value;
                            var min = regexMatch.Groups["min"].Value;
                            var second = regexMatch.Groups["second"].Value;
                            var diagTZ = regexMatch.Groups["timezone"].Value;
                            DateTime diagDateTime = new DateTime(int.Parse(year), int.Parse(month), int.Parse(day),
                                                                    int.Parse(hour), int.Parse(min), int.Parse(second));
                            var tzInstance = StringHelpers.FindTimeZone(diagTZ);

                            captureDateTime = tzInstance == null ? diagDateTime.ConvertToOffset() : Common.TimeZones.ConvertToOffset(diagDateTime, tzInstance);                            
                            break;
                        }
                    }
                }
                catch
                { }
            }

            return captureDateTime;
        }
    }
}
