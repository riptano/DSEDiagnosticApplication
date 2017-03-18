using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Common;
using Newtonsoft.Json;
using DSEDiagnosticLibrary;

namespace DSEDiagnosticFileParser
{
    internal class cql_ddl : DiagnosticFile
    {
        public cql_ddl(CatagoryTypes catagory,
                            IDirectoryPath diagnosticDirectory,
                            IFilePath file,
                            INode node,
                            string defaultClusterName,
                            string defaultDCName)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName)
        {
            if(this.Node != null)
            {
                this.Cluster = this.Node.Cluster;
            }

            if(this.Cluster == null)
            {
                this.Cluster = Cluster.GetCurrentOrMaster();
            }

            this._result = new DDLResult(this);
        }

        public sealed class DDLResult : IResult
        {
            private DDLResult() { }
            internal DDLResult(cql_ddl fileInstance)
            {
                this.Path = fileInstance.File;
                this.Node = fileInstance.Node;
                this._ddlList = fileInstance._ddlList;
            }

            private readonly List<IDDL> _ddlList;

            #region IResult
            public IPath Path { get; private set; }
            public Cluster Cluster { get { return this.DataCenter?.Cluster; } }
            public IDataCenter DataCenter { get { return this.Node?.DataCenter; } }
            public INode Node { get; private set; }
            public int NbrItems { get { return this._ddlList.Count; } }

            public IEnumerable<IParsed> Results { get { return this._ddlList; } }

            #endregion
        }

        private readonly DDLResult _result;

        public Cluster Cluster { get; private set; }

        public override IResult GetResult()
        {
            return this._result;
        }

        // RegEx: 0
        // use keyspace
        // null, keyspace, null
        //
        // RegEx: 1
        // CREATE  KEYSPACE | SCHEMA  [IF NOT EXISTS] keyspace_name WITH REPLICATION = map [AND DURABLE_WRITES = true | false]
        // { 'class' : 'SimpleStrategy', 'replication_factor' : <integer> }
        // { 'class' : 'NetworkTopologyStrategy'[, '<data center>' : <integer>, '<data center>' : <integer>] . . . }
        // null, keyspace_name, replication, { ... }, durable_write, true|false, null
        // null, keyspace_name, replication, { ... }, null
        //
        // RegEx: 2
        // CREATE TABLE [IF NOT EXISTS] [keyspace_name.]table_name ( column_definition[, ...] PRIMARY KEY(column_name[, column_name...]) [WITH table_options | CLUSTERING ORDER BY(clustering_column_name order]) | ID = 'table_hash_tag' | COMPACT STORAGE]
        // null, [keyspace_name.]table_name, column_definition_clause, with_options, null
        // null, [keyspace_name.]table_name, column_definition_clause, null

        private List<IDDL> _ddlList = new List<IDDL>();
        private List<IKeyspace> _localKS = new List<IKeyspace>();

        private IKeyspace _usingKeySpace = null;

        public override uint ProcessFile()
        {
            List<string> cqlStatements = new List<string>();
            StringBuilder strCQL = new StringBuilder();
            uint nbrGenerated = 0;
            uint itemNbr = 0;
            bool multipleLineComment = false;
            string line;

            foreach (var fileLine in this.File.ReadAllLines())
            {
                line = fileLine.Trim();

                if(multipleLineComment)
                {
                    if(line.EndsWith(@"*/"))
                    {
                        multipleLineComment = false;
                    }
                    continue;
                }

                if (!string.IsNullOrEmpty(line))
                {
                    if (line.StartsWith(@"--") || line.StartsWith(@"//"))
                    {
                        continue;
                    }
                    var startComment = line.LastIndexOf(@"/*");

                    if(startComment >= 0)
                    {
                        var endComment = line.Substring(startComment + 1).LastIndexOf(@"*/");

                        if(endComment < 0)
                        {
                            multipleLineComment = true;
                            continue;
                        }

                        if (endComment + 2 < line.Length)
                        {
                            line = (line.Substring(0, startComment) + " " + line.Substring(endComment + 2)).TrimEnd();
                        }
                        else
                        {
                            line = line.Substring(0, startComment).TrimEnd();
                        }
                    }

                    strCQL.Append(line + " ");

                    if (line.TrimEnd().Last() == ';')
                    {
                        cqlStatements.Add(strCQL.ToString().TrimEnd());
                        strCQL.Clear();
                    }
                }
            }

            if(cqlStatements.Count == 0)
            {
                this.Processed = false;
                return 0;
            }

            Match regexMatch;

            foreach (var cqlStmt in cqlStatements)
            {
                ++itemNbr;
                regexMatch = this.RegExParser.Match(cqlStmt, 0); //cql use

                if(regexMatch.Success)
                {
                    var keyspaceName = regexMatch.Groups[1].Value.Trim();
                    this._usingKeySpace = this.GetKeySpace(keyspaceName);

                    if (this._usingKeySpace == null)
                    {
                        Logger.Instance.ErrorFormat("<NoNodeId>\t{0}\tUsing Keyspace \"{1}\" was not found during CQL DDL. Keyspaces need to be defined before a using CQL statement.", this.File.PathResolved, keyspaceName);
                    }
                    continue;
                }

                regexMatch = this.RegExParser.Match(cqlStmt, 1); //create keyspace

                if (regexMatch.Success)
                {
                    if(ProcessDDLKeyspace(regexMatch, itemNbr))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }
            }

            this.NbrItemsParsed = (int) itemNbr;
            this.Processed = true;
            return nbrGenerated;
        }

        private IKeyspace GetKeySpace(string ksName)
        {
            var ksInstance = this._localKS.FirstOrDefault(k => k.Name == ksName);

            if(ksInstance == null)
            {
                ksInstance = this.Cluster.GetKeyspaces(ksName).FirstOrDefault();
            }

            return ksInstance;
        }

        private bool AddKeySpace(IKeyspace keySpace)
        {
            var ksInstance = this._localKS.FirstOrDefault(k => k.Name == keySpace.Name);

            if (ksInstance == null)
            {
                ksInstance = this.Cluster.AssociateItem(keySpace);
                this._localKS.Add(ksInstance);
                this._ddlList.Add(ksInstance);

                return ReferenceEquals(keySpace, ksInstance);
            }
            
            return false;
        }

        private bool ProcessDDLKeyspace(Match keyspaceMatch, uint lineNbr)
        {
            var name = keyspaceMatch.Groups[1].Value.Trim();
            var replicationMap = JsonConvert.DeserializeObject<Dictionary<string, string>>(keyspaceMatch.Groups[3].Value);
            var durableWrites = keyspaceMatch.Groups[4].Value.ToUpper() == "DURABLE_WRITES" ? bool.Parse(keyspaceMatch.Groups[5].Value) : true;
            var strategy = replicationMap.FirstOrDefault().Value;
            var replications = replicationMap.Skip(1)
                                    .Select(r => new KeyspaceReplicationInfo(int.Parse(r.Value),
                                                                                r.Key.ToLower() == "replication_factor"
                                                                                    ? this.Node?.DataCenter
                                                                                    : this.Cluster.TryGetDataCenter(r.Key)));

            var keyspace = new KeySpace(this.File,
                                            lineNbr,
                                            name,
                                            strategy,
                                            replications,
                                            durableWrites,
                                            keyspaceMatch.Groups[0].Value,
                                            this.Node,
                                            this.Cluster);

            return this.AddKeySpace(keyspace);
        }
    }
}
