using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Newtonsoft.Json;
using Common;
using Common.Path;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    [JsonObject(MemberSerialization.OptOut)]
    public sealed class file_return_collection : DiagnosticFile
    {

        private IFilePath _fileWildPath = null;
        private SourceTypes _sourceType = SourceTypes.Unknown;
        private Cluster _cluster = null;
        private IDataCenter _dataCenter = null;

        public file_return_collection(CatagoryTypes catagory,
                                        IDirectoryPath diagnosticDirectory,
                                        IFilePath file,
                                        INode node,
                                        string defaultClusterName,
                                        string defaultDCName,
                                        Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this._result = new FileCollectionResult(this);
            this._fileWildPath = file;

            switch (catagory)
            {
                case CatagoryTypes.Unknown:
                    break;
                case CatagoryTypes.ConfigurationFile:
                    break;
                case CatagoryTypes.LogFile:
                    this._sourceType = SourceTypes.CassandraLog;
                    break;
                case CatagoryTypes.AchievedLogFile:
                    this._sourceType = SourceTypes.CassandraLog;
                    break;
                case CatagoryTypes.CommandOutputFile:
                    break;
                case CatagoryTypes.SystemOutputFile:
                    this._sourceType = SourceTypes.EnvFile;
                    break;
                case CatagoryTypes.CQLFile:
                    this._sourceType = SourceTypes.CQL;
                    break;
                case CatagoryTypes.ZipFile:
                    break;
                case CatagoryTypes.TransformFile:
                    break;
                default:
                    break;
            }

            this._cluster = this.Node?.Cluster;
            this._dataCenter = this.Node?.DataCenter;

            if (this._cluster == null)
            {
                if (!string.IsNullOrEmpty(this.DefaultClusterName))
                {
                    this._cluster = Cluster.TryGetCluster(this.DefaultClusterName);
                }

                if (this._cluster == null)
                {
                    this._cluster = Cluster.GetCurrentOrMaster();
                }
            }

            if (this._dataCenter == null && !string.IsNullOrEmpty(this.DefaultDataCenterName))
            {
                this._dataCenter = Cluster.TryGetDataCenter(this.DefaultDataCenterName, this._cluster);
            }
        }

        public file_return_collection(IFilePath file,
                                        string defaultClusterName,
                                        string defaultDCName,
                                        Version targetDSEVersion = null)
            : base(CatagoryTypes.CQLFile, file, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this._result = new FileCollectionResult(this);
            this._fileWildPath = file;
            this._cluster = this.Node?.Cluster;
            this._dataCenter = this.Node?.DataCenter;

            if (this._cluster == null)
            {
                if (!string.IsNullOrEmpty(this.DefaultClusterName))
                {
                    this._cluster = Cluster.TryGetCluster(this.DefaultClusterName);
                }

                if (this._cluster == null)
                {
                    this._cluster = Cluster.GetCurrentOrMaster();
                }
            }

            if (this._dataCenter == null && !string.IsNullOrEmpty(this.DefaultDataCenterName))
            {
                this._dataCenter = Cluster.TryGetDataCenter(this.DefaultDataCenterName, this._cluster);
            }
        }

        [JsonObject(MemberSerialization.OptOut)]
        public sealed class FileItem : IParsed
        {

            internal FileItem(IFilePath filePath, file_return_collection instance)
            {
                this.Path = filePath;
                this.Source = instance._sourceType;

                if(filePath != null)
                {
                    var nodeId = DetermineNodeIdentifier(filePath, -1);

                    if(nodeId == null)
                    {
                        this.Node = instance.Node;
                    }
                    else
                    {
                        this.Node = Cluster.TryGetAddNode(nodeId,
                                                            instance._dataCenter?.Name ?? instance.DefaultDataCenterName,
                                                            instance._cluster?.Name ?? instance.DefaultClusterName);
                    }
                }

                this.Cluster = this.Node?.Cluster ?? instance._cluster;
                this.DataCenter = this.Node?.DataCenter ?? instance._dataCenter;
            }

            public SourceTypes Source { get; }
            [JsonConverter(typeof(DSEDiagnosticLibrary.IPathJsonConverter))]
            public IPath Path { get; }
            public Cluster Cluster { get; }
            public IDataCenter DataCenter { get; }
            public INode Node { get; }
            public int Items { get; } = 1;
            public uint LineNbr { get; } = 0;
        }


        [JsonObject(MemberSerialization.OptOut)]
        public sealed class FileCollectionResult : IResult
        {
            internal FileCollectionResult(file_return_collection instance)
            {
                this.Path = instance.File;
                this.Node = instance.Node;
                this.Cluster = instance.Node?.Cluster;
                this.DataCenter = instance.Node?.DataCenter;

                if (this.Cluster == null)
                {
                    this.Cluster = instance._cluster;
                }

                if (this.DataCenter == null)
                {
                    this.DataCenter = instance._dataCenter;
                }
            }

            [JsonConverter(typeof(DSEDiagnosticLibrary.IPathJsonConverter))]
            public IPath Path { get; }
            public Cluster Cluster { get; }
            public IDataCenter DataCenter { get; }
            public INode Node { get; }
            public int NbrItems { get { return this._fileItems?.Count ?? 0; } }

            [JsonProperty(PropertyName = "Results")]
            internal List<FileItem> _fileItems = null;

            [JsonIgnore]
            public IEnumerable<IParsed> Results
            {
                get { return this._fileItems ?? Enumerable.Empty<FileItem>(); }
            }
        }

        [JsonProperty(PropertyName = "Results")]
        private FileCollectionResult _result;

        public override IResult GetResult()
        {
            return this._result;
        }

        public override uint ProcessFile()
        {
            if(this._fileWildPath == null)
            {
                return 0;
            }

            if (this._fileWildPath.HasWildCardPattern())
            {
                this._result._fileItems = this._fileWildPath.GetWildCardMatches()
                                                .Where(f => f.IsFilePath)
                                                .Cast<IFilePath>()
                                                .Select(f => new FileItem(f, this)).ToList();
            }
            else
            {
                this._result._fileItems = new List<FileItem>() { new FileItem(this._fileWildPath, this) };
            }

            return (uint)this._result._fileItems.Count;
        }
    }
}
