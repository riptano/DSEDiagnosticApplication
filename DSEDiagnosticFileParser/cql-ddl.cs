﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Common;
using Newtonsoft.Json;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticFileParser
{
    public class cql_ddl : DiagnosticFile
    {

        public static void DisabledSystemDSEDefaultKeyspacesInitialLoading()
        {
            SystemDDLInitialized = true;
            LoadSystemDSEDefaultKeyspaces = false;
        }

        public static bool LoadSystemDSEKeyspaces
        {
            get { return LoadSystemDSEDefaultKeyspaces; }           
        }

        static bool LoadSystemDSEDefaultKeyspaces = true;
        volatile static bool SystemDDLInitialized = false;
        static object syncLock = new object();

        static cql_ddl()
        {
            var systemDDL = new cql_ddl(Cluster.MasterCluster,
                                            Properties.Settings.Default.DSESystemDDL,
                                            null,
                                            null,
                                            true,
                                            true);
        }

        private void InitializeInstance()
        {
            if (this.Node != null)
            {
                this.Cluster = this.Node.Cluster;
            }

            if (this.Cluster == null)
            {
                if (!string.IsNullOrEmpty(this.DefaultClusterName))
                {
                    this.Cluster = Cluster.TryGetCluster(this.DefaultClusterName);
                }

                if (this.Cluster == null)
                {
                    this.Cluster = Cluster.GetCurrentOrMaster();
                }
            }

            if (this.Node != null)
            {
                this.DataCenter = this.Node.DataCenter;
            }

            if (this.DataCenter == null && !string.IsNullOrEmpty(this.DefaultDataCenterName))
            {
                this.DataCenter = Cluster.TryGetDataCenter(this.DefaultDataCenterName, this.Cluster);
            }

            if(!SystemDDLInitialized)
            {
                lock (syncLock)
                {
                    if (!SystemDDLInitialized)
                    {
                        SystemDDLInitialized = true;

                        var systemDDL = new cql_ddl(Cluster.MasterCluster, null, null, null, true, true);
                        var ddlLines = Properties.Settings.Default.DSESystemDDL.Split(new string[] { Environment.NewLine }, StringSplitOptions.None);

                        systemDDL.ProcessFile(ddlLines);                        
                    }
                }
            }
        }
        public cql_ddl(CatagoryTypes catagory,
                            IDirectoryPath diagnosticDirectory,
                            IFilePath file,
                            INode node,
                            string defaultClusterName,
                            string defaultDCName,
                            Version targetDSEVersion)
            : base(catagory, diagnosticDirectory, file, node, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this.InitializeInstance();
            this._result = new DDLResult(this);
        }

        public cql_ddl(IFilePath file,
                            string defaultClusterName,
                            string defaultDCName,
                            Version targetDSEVersion = null)
            : base(CatagoryTypes.CQLFile, file, defaultClusterName, defaultDCName, targetDSEVersion)
        {
            this.InitializeInstance();
            this._result = new DDLResult(this);
        }

        public cql_ddl(Cluster cluster,
                        string cqlString,
                            IDataCenter dataCenter = null,
                            Version targetDSEVersion = null,
                            bool preLoaded = false,
                            bool systemDDLInitialization = false)
            : base(CatagoryTypes.CQLFile, null, cluster?.Name, dataCenter?.Name, targetDSEVersion)
        {           
            this.Cluster = cluster;
            this.DataCenter = dataCenter;

            this._result = new DDLResult(this);

            this._isPreLoaded = preLoaded;

            if (!string.IsNullOrEmpty(cqlString))
            {
                var convertString = cqlString.Replace(";", ";" + Environment.NewLine);
                var ddlLines = cqlString.Split(new string[] { Environment.NewLine }, StringSplitOptions.None);

                if (systemDDLInitialization)
                {
                    if (!SystemDDLInitialized)
                    {
                        lock (syncLock)
                        {
                            if (!SystemDDLInitialized)
                            {
                                SystemDDLInitialized = true;

                                this.ProcessFile(ddlLines);
                            }
                        }
                    }
                }
                else
                {
                    this.ProcessFile(ddlLines);
                }
            }
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
        public IDataCenter DataCenter { get; private set; }

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
        // CREATE TABLE [IF NOT EXISTS] [keyspace_name.]table_name ( column_definition[, ...] PRIMARY KEY(column_name[, column_name...])) [WITH table_options | CLUSTERING ORDER BY(clustering_column_name order]) | ID = 'table_hash_tag' | COMPACT STORAGE]
        // null, [keyspace_name.]table_name, column_definition_clause, with_options, null
        // null, [keyspace_name.]table_name, null, null, column_definition_clause, null
        //
        // RegEx: 3
        // CREATE TYPE [IF NOT EXISTS] [keyspace.]type_name ( field, field, ...)
        // null, [keyspace_name.]table_name, column_definition_clause, null
        //
        // RegEx: 4
        // CREATE [CUSTOM] INDEX [IF NOT EXISTS] index_name ON [keyspace_name.]table_name ( column_name, [KEYS ( column_name )] ) [USING class_name] [WITH OPTIONS = json-map]
        // null, [CUSTOM], index_name, [keyspace_name.]table_name, column_definition_clause, [class_name], [json-map], null
        //
        // RegEx: 5
        // CREATE MATERIALIZED VIEW [IF NOT EXISTS] [keyspace_name.] view_name AS SELECT column_list FROM[keyspace_name.] base_table_name WHERE column_name IS NOT NULL[AND column_name IS NOT NULL...] [AND relation...] PRIMARY KEY(column_list ) [WITH [table_properties] [AND CLUSTERING ORDER BY(cluster_column_name order_option)]]
        // null, [keyspace_name.] view_name, column_list, [keyspace_name.]base_table, where clause, PRIMARY KEY column_list, with cluase, null
        // null, [keyspace_name.] view_name, column_list, [keyspace_name.]base_table, where clause, null, null, PRIMARY KEY column_list, null
        //
        // RegEx: 6
        // CREATE TRIGGER IF NOT EXISTS trigger_name ON [keyspace_name.]table_name USING 'java_class'
        // null, trigger_name, [keyspace_name.]table_name, java_class, null


        private List<IDDL> _ddlList = new List<IDDL>();
        private List<IKeyspace> _localKS = new List<IKeyspace>();
        private bool _isPreLoaded = false;

        private IKeyspace _usingKeySpace = null;

        public override uint ProcessFile()
        {
            return this.ProcessFile(this.File.ReadAllLines());
        }

        public uint ProcessFile(string[] lines)
        {
            List<string> cqlStatements = new List<string>();
            StringBuilder strCQL = new StringBuilder();
            uint nbrGenerated = 0;
            bool multipleLineComment = false;
            string line;

            foreach (var fileLine in lines)
            {
                this.CancellationToken.ThrowIfCancellationRequested();

                line = fileLine.Trim();

                if (multipleLineComment)
                {
                    string newLine;
                    int inlinecomment = StringHelpers.RemoveInLineComment(line, out newLine, -1, true);

                    if (inlinecomment == 0)
                    {
                        continue;
                    }
                    else if (inlinecomment == 1)
                    {
                        multipleLineComment = false;
                    }

                    line = newLine.Trim();

                    if (line == string.Empty)
                    {
                        continue;
                    }
                }

                if (line.Length > 0)
                {
                    if (line[0] == '\u001b' || line[0] == '\n' || line[0] == '\r')
                    {
                        continue;
                    }

                    if (line.StartsWith(@"--")
                            || line.StartsWith(@"//")
                            || line.StartsWith("warning: ", StringComparison.CurrentCultureIgnoreCase)
                            || line.StartsWith("error: ", StringComparison.CurrentCultureIgnoreCase))
                    {
                        continue;
                    }

                    {
                        string newLine;
                        int inlinecomment = StringHelpers.RemoveInLineComment(line, out newLine, -1, true);

                        if (inlinecomment != 0)
                        {
                            if (inlinecomment == -1)
                            {
                                multipleLineComment = true;
                            }

                            line = newLine.Trim();

                            if (line == string.Empty)
                            {
                                continue;
                            }
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

            if (cqlStatements.Count == 0)
            {
                this.Processed = false;
                return 0;
            }

            Match regexMatch;

            foreach (var cqlStmt in cqlStatements)
            {
                ++this.NbrItemsParsed;
                regexMatch = this.RegExParser.Match(cqlStmt, 0); //cql use

                if (regexMatch.Success)
                {
                    var keyspaceName = regexMatch.Groups[1].Value.Trim();
                    this._usingKeySpace = this.GetKeySpace(keyspaceName);

                    if (this._usingKeySpace == null)
                    {
                        Logger.Instance.ErrorFormat("<NoNodeId>\t{0}\tUsing Keyspace \"{1}\" was not found during CQL DDL. Keyspaces need to be defined before a using CQL statement.", this.ShortFilePath, keyspaceName);
                        ++this.NbrErrors;
                    }
                    continue;
                }

                regexMatch = this.RegExParser.Match(cqlStmt, 1); //create keyspace

                if (regexMatch.Success)
                {
                    if (ProcessDDLKeyspace(regexMatch, (uint)this.NbrItemsParsed))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }

                regexMatch = this.RegExParser.Match(cqlStmt, 2); //create table

                if (regexMatch.Success)
                {
                    if (ProcessDDLTable(regexMatch, (uint)this.NbrItemsParsed))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }

                regexMatch = this.RegExParser.Match(cqlStmt, 3); //create type

                if (regexMatch.Success)
                {
                    if (ProcessDDLUDT(regexMatch, (uint)this.NbrItemsParsed))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }

                regexMatch = this.RegExParser.Match(cqlStmt, 4); //create index

                if (regexMatch.Success)
                {
                    if (ProcessDDLIndex(regexMatch, (uint)this.NbrItemsParsed))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }

                regexMatch = this.RegExParser.Match(cqlStmt, 5); //create materialized view

                if (regexMatch.Success)
                {
                    if (ProcessDDLMaterializediew(regexMatch, (uint)this.NbrItemsParsed))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }

                regexMatch = this.RegExParser.Match(cqlStmt, 6); //create trigger

                if (regexMatch.Success)
                {
                    if (ProcessDDLTrigger(regexMatch, (uint)this.NbrItemsParsed))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }

                regexMatch = this.RegExParser.Match(cqlStmt, 7); //create function

                if (regexMatch.Success)
                {
                    if (ProcessDDLFunction(regexMatch, (uint)this.NbrItemsParsed))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }

                Logger.Instance.ErrorFormat("{0}\t{1}\tCQL Statement Processing Error (DC {3}). Unknown CQL statement of \"{2}\".",
                                                this.Node,
                                                this.ShortFilePath,
                                                cqlStmt,
                                                this.Node?.DataCenter?.Name);
                ++this.NbrErrors;
            }

            this.Processed = true;
            return nbrGenerated;
        }

        private IKeyspace GetKeySpace(string ksName)
        {
            var ksInstance = this._localKS.FirstOrDefault(k => k.Name == ksName);

            if (ksInstance == null)
            {
                var dcName = this.Node?.DataCenter.Name ?? this.DefaultDataCenterName;

                if (dcName != null)
                {
                    ksInstance = this.Cluster.GetKeyspaces(ksName).FirstOrDefault(k => k.EverywhereStrategy || k.LocalStrategy || k.Replications.Any(r => r.DataCenter.Name == dcName));
                }
            }

            return ksInstance;
        }

        private bool AddKeySpace(IKeyspace keySpace)
        {
            var ksInstance = this._localKS.FirstOrDefault(k => k.Name == keySpace.Name);

            if (ksInstance == null)
            {
                Cluster localCluster = this.Cluster;

                if(localCluster.IsMaster
                        && keySpace.Cluster != null
                        && !keySpace.Cluster.IsMaster)
                {
                    localCluster = keySpace.Cluster;
                }

                ksInstance = localCluster.AssociateItem(keySpace);
                this._localKS.Add(ksInstance);

                if (ReferenceEquals(keySpace, ksInstance))
                {
                    this._ddlList.Add(keySpace);
                    return true;
                }
            }

            return false;
        }

        private bool ProcessDDLKeyspace(Match keyspaceMatch, uint lineNbr)
        {
            /*
             CREATE KEYSPACE Excelsior
                WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 3 };

             CREATE KEYSPACE "Excalibur"
                WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'dc1' : 3, 'dc2' : 2};
            */
            var name = keyspaceMatch.Groups[1].Value.Trim();
            var replicationMap = JsonConvert.DeserializeObject<Dictionary<string, string>>(keyspaceMatch.Groups[3].Value);
            var durableWrites = keyspaceMatch.Groups[4].Success && keyspaceMatch.Groups[4].Value.ToUpper() == "DURABLE_WRITES"
                                        ? bool.Parse(keyspaceMatch.Groups[5].Value)
                                        : true;
            var isPreLoaded = keyspaceMatch.Groups[6].Success && keyspaceMatch.Groups[6].Value.ToUpper() == "PRELOADED"
                                        ? bool.Parse(keyspaceMatch.Groups[7].Value)
                                        : this._isPreLoaded;
            var strategy = replicationMap.FirstOrDefault().Value;
            var replications = replicationMap.Skip(1)
                                    .Select(r => new KeyspaceReplicationInfo(int.Parse(r.Value),
                                                                                r.Key.ToLower() == "replication_factor"
                                                                                    ? this.DataCenter //SimpleStrategy
                                                                                    : this.Cluster.TryGetDataCenter(r.Key) ?? Cluster.TryGetAddDataCenter(r.Key, this.Cluster)));

            var keyspace = new KeySpace(this.File,
                                            lineNbr,
                                            name,
                                            strategy,
                                            replications,
                                            durableWrites,
                                            keyspaceMatch.Groups[0].Value,
                                            this.Node,
                                            this.Cluster,
                                            isPreLoaded);

            return this.AddKeySpace(keyspace);
        }

        static readonly Regex WithCompactStorageRegEx = new Regex(@"(?:\s|^)compact\s+storage(?:\s|\;|$)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        static readonly Regex WithOrderByRegEx = new Regex(@"(?:\s|^)clustering\s+order\s+by\s*\(([a-z0-9\-_$%+=@!?^*&,\ ]+)\)(?:\s|\;|$)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private bool ProcessDDLTable(Match tableMatch, uint lineNbr)
        {
            var name = tableMatch.Groups[1].Value.Trim();
            var kstblPair = StringHelpers.SplitTableName(name, null);
            var ksInstance = string.IsNullOrEmpty(kstblPair.Item1) ? this._usingKeySpace : this.GetKeySpace(kstblPair.Item1);

            if (ksInstance == null) throw new ArgumentNullException("keyspace", string.Format("CQL Table \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                 kstblPair.Item2,
                                                                                                 kstblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                 tableMatch.Groups[0].Value));

            if (ksInstance.TryGetTable(kstblPair.Item2) == null)
            {
                var strColumns = tableMatch.Groups[2].Success ? tableMatch.Groups[2].Value.Trim() : tableMatch.Groups[4].Value.Trim();
                var strWithClause = tableMatch.Groups[3].Success ? tableMatch.Groups[3].Value.Trim() : null;
                var columnsSplit = Common.StringFunctions.Split(strColumns,
                                                                    ',',
                                                                    StringFunctions.IgnoreWithinDelimiterFlag.AngleBracket
                                                                        | StringFunctions.IgnoreWithinDelimiterFlag.Parenthese,
                                                                    StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
                var columns = this.ProcessTableColumns(columnsSplit, ksInstance);
                var properties = new Dictionary<string, object>();
                var orderByList = new List<CQLOrderByColumn>();

                ProcessWithOptions(strWithClause, columns, orderByList, properties);

                var cqlTable = new CQLTable(this.File,
                                                lineNbr,
                                                ksInstance,
                                                kstblPair.Item2,
                                                columns,
                                                null,
                                                null,
                                                orderByList,
                                                properties,
                                                tableMatch.Value,
                                                this.Node);

                this._ddlList.Add(cqlTable);

                return true;
            }

            return false;
        }

        private bool ProcessDDLUDT(Match udtMatch, uint lineNbr)
        {
            var name = udtMatch.Groups[1].Value.Trim();
            var kstblPair = StringHelpers.SplitTableName(name, null);
            var ksInstance = string.IsNullOrEmpty(kstblPair.Item1) ? this._usingKeySpace : this.GetKeySpace(kstblPair.Item1);

            if (ksInstance == null) throw new ArgumentNullException("keyspace", string.Format("CQL Type (UDT) \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                 kstblPair.Item2,
                                                                                                 kstblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                 udtMatch.Groups[0].Value));

            if (ksInstance.TryGetUDT(kstblPair.Item2) == null)
            {
                var strColumns = udtMatch.Groups[2].Value.Trim();
                var columnsSplit = Common.StringFunctions.Split(strColumns,
                                                                    ',',
                                                                    StringFunctions.IgnoreWithinDelimiterFlag.AngleBracket
                                                                        | StringFunctions.IgnoreWithinDelimiterFlag.Parenthese,
                                                                    StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
                var columns = this.ProcessTableColumns(columnsSplit, ksInstance);

                var cqlUDT = new CQLUserDefinedType(this.File,
                                                        lineNbr,
                                                        ksInstance,
                                                        kstblPair.Item2,
                                                        columns,
                                                        udtMatch.Value,
                                                        this.Node);

                this._ddlList.Add(cqlUDT);

                return true;
            }

            return false;
        }

        private bool ProcessDDLIndex(Match indexMatch, uint lineNbr)
        {
            var name = indexMatch.Groups[2].Value.Trim();
            var ksidxPair = StringHelpers.SplitTableName(name, null);
            var tablename = indexMatch.Groups[3].Value.Trim();
            var kstblPair = StringHelpers.SplitTableName(tablename, null);
            var ksTblInstance = string.IsNullOrEmpty(kstblPair.Item1) ? this._usingKeySpace : this.GetKeySpace(kstblPair.Item1);
            var ksInstance = string.IsNullOrEmpty(ksidxPair.Item1) ? ksTblInstance : this.GetKeySpace(ksidxPair.Item1);

            if (ksTblInstance == null) throw new ArgumentNullException("TableKeyspace", string.Format("CQL Index \"{0}\" for Table \"{1}\" could not find associated keyspace \"{2}\". DDL: {3}",
                                                                                                         name,
                                                                                                         tablename,
                                                                                                         kstblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                         indexMatch.Groups[0].Value));
            if (ksInstance == null) throw new ArgumentNullException("Keyspace", string.Format("CQL Index \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                 name,
                                                                                                 ksidxPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                 indexMatch.Groups[0].Value));

            var tblInstance = ksTblInstance.TryGetTable(kstblPair.Item2);

            if (tblInstance == null) throw new ArgumentNullException("Table", string.Format("CQL Index \"{0}\" could not find associated Table \"{1}\" in Keyspace \"{2}\". DDL: {3}",
                                                                                             name,
                                                                                             tablename,
                                                                                             ksInstance.Name,
                                                                                             indexMatch.Groups[0].Value));


            if (ksInstance.TryGetIndex(ksidxPair.Item2) == null)
            {
                var strColumns = indexMatch.Groups[4].Value.Trim();
                var columnsSplit = Common.StringFunctions.Split(strColumns,
                                                                    ',',
                                                                    StringFunctions.IgnoreWithinDelimiterFlag.AngleBracket
                                                                        | StringFunctions.IgnoreWithinDelimiterFlag.Parenthese,
                                                                    StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
                var colFunctions = columnsSplit.Select(i =>
                                    {
                                        List<string> columns = new List<string>();
                                        string functionName = null;

                                        if(i.Last() == ')')
                                        {
                                            Common.StringFunctions.ParseIntoFuncationParams(i, out functionName, out columns);
                                        }
                                        else
                                        {
                                            columns.Add(i);
                                        }

                                        return new CQLFunctionColumn(functionName, tblInstance.TryGetColumns(columns), i);
                                    });

                var usingClass = indexMatch.Groups[5].Success
                                    ? indexMatch.Groups[5].Value.Trim()
                                    : null;
                var withOptions = indexMatch.Groups[6].Success
                                    ? indexMatch.Groups[6].Value.Trim()
                                    : null;

                var cqlIdx = new CQLIndex(this.File,
                                            lineNbr,
                                            ksidxPair.Item2,
                                            tblInstance,
                                            colFunctions,
                                            indexMatch.Groups[1].Success && indexMatch.Groups[1].Value.ToLower() == "custom",
                                            usingClass,
                                            string.IsNullOrEmpty(withOptions)
                                                ? null
                                                : JsonConvert.DeserializeObject<Dictionary<string,object>>(withOptions),
                                            indexMatch.Value,
                                            this.Node);

                this._ddlList.Add(cqlIdx);

                return true;
            }

            return false;
        }

        private bool ProcessDDLMaterializediew(Match viewMatch, uint lineNbr)
        {
            var viewName = viewMatch.Groups[1].Value.Trim();
            var viewKsTblPair = StringHelpers.SplitTableName(viewName, null);
            var tblName = viewMatch.Groups[3].Value.Trim();
            var tblKsTblPair = StringHelpers.SplitTableName(tblName, null);
            var tblKSInstance = string.IsNullOrEmpty(tblKsTblPair.Item1) ? this._usingKeySpace : this.GetKeySpace(viewKsTblPair.Item1);
            var viewKSInstance = string.IsNullOrEmpty(viewKsTblPair.Item1) ? tblKSInstance : this.GetKeySpace(viewKsTblPair.Item1);

            if (viewKSInstance == null) throw new ArgumentNullException("Keyspace", string.Format("CQL Materialized View \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                         viewKsTblPair.Item2,
                                                                                                         viewKsTblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                         viewMatch.Groups[0].Value));

            if (tblKSInstance == null) throw new ArgumentNullException("TableKeyspace", string.Format("CQL Materialized View \"{0}\" could not find associated table keyspace \"{1}\". DDL: {2}",
                                                                                                        tblKsTblPair.Item2,
                                                                                                        tblKsTblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                        viewMatch.Groups[0].Value));
            var tblInstance = tblKSInstance.TryGetTable(tblKsTblPair.Item2);

            if (tblInstance == null) throw new ArgumentNullException("Table", string.Format("CQL Materialized View \"{0}\" could not find associated Table \"{1}\" in Keyspace \"{2}\". DDL: {3}",
                                                                                             viewName,
                                                                                             tblKsTblPair.Item2,
                                                                                             tblKSInstance.Name,
                                                                                             viewMatch.Groups[0].Value));

            if (viewKSInstance.TryGetView(viewKsTblPair.Item2) == null)
            {
                var strColumns = viewMatch.Groups[2].Value.Trim();
                var strPrimaryKeys = viewMatch.Groups[5].Success ? viewMatch.Groups[5].Value.Trim() : viewMatch.Groups[7].Value.Trim();
                var strWithClause = viewMatch.Groups[6].Success ? viewMatch.Groups[6].Value.Trim() : null;
                var strWhereClause = viewMatch.Groups[4].Value.Trim();
                var columnsSplit = strColumns == "*"
                                    ? null
                                    : strColumns.Split(',');
                IEnumerable<ICQLColumn> columns;
                var pkList = new List<ICQLColumn>();
                var ckList = new List<ICQLColumn>();

                {
                    var pkValues = Common.StringFunctions.Split(strPrimaryKeys,
                                                                        ',',
                                                                        StringFunctions.IgnoreWithinDelimiterFlag.AngleBracket
                                                                            | StringFunctions.IgnoreWithinDelimiterFlag.Parenthese,
                                                                        StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
                    ICQLColumn cqlColumn;

                    if (pkValues[0][0] == '(')
                    {
                        var pkCols = pkValues[0].Substring(1, pkValues[0].Length - 2).Split(',');

                        foreach (var pkCol in pkCols)
                        {
                            cqlColumn = tblInstance.TryGetColumn(pkCol)?.Copy();
                            ((CQLColumn)cqlColumn).IsPrimaryKey = true;
                            pkList.Add(cqlColumn);
                        }
                    }
                    else
                    {
                        cqlColumn = tblInstance.TryGetColumn(pkValues[0])?.Copy();
                        ((CQLColumn)cqlColumn).IsPrimaryKey = true;
                        pkList.Add(cqlColumn);
                    }

                    foreach (var clusterCol in pkValues.Skip(1))
                    {
                        cqlColumn = tblInstance.TryGetColumn(clusterCol)?.Copy();
                        ((CQLColumn)cqlColumn).IsClusteringKey = true;
                        ckList.Add(cqlColumn);
                    }

                    if (columnsSplit == null)
                    {
                        columns = tblInstance.Columns.Select(c => c.Copy()).ToArray();
                    }
                    else
                    {
                        columns = pkList.Concat(ckList.ToArray())
                                        .Concat(tblInstance.TryGetColumns(columnsSplit)
                                                        .Where(c => !(pkList.Any(p => p.Name == c.Name)
                                                                            || ckList.Any(k => k.Name == c.Name)))
                                                        .Select(c => c.Copy()).ToArray());
                    }
                }

                var properties = new Dictionary<string, object>();
                var orderByList = new List<CQLOrderByColumn>();

                ProcessWithOptions(strWithClause, columns.Cast<CQLColumn>(), orderByList, properties);

                var cqlView = new CQLMaterializedView(this.File,
                                                        lineNbr,
                                                        viewKSInstance,
                                                        viewKsTblPair.Item2,
                                                        tblInstance,
                                                        columns,
                                                        pkList,
                                                        ckList,
                                                        orderByList,
                                                        properties,
                                                        strWhereClause,
                                                        viewMatch.Value,
                                                        this.Node);

                this._ddlList.Add(cqlView);

                return true;
            }

            return false;
        }

        private bool ProcessDDLTrigger(Match triggerMatch, uint lineNbr)
        {
            var name = triggerMatch.Groups[1].Value.Trim();
            var kstriggerPair = StringHelpers.SplitTableName(name, null);
            var tablename = triggerMatch.Groups[2].Value.Trim();
            var kstblPair = StringHelpers.SplitTableName(tablename, null);
            var ksTblInstance = string.IsNullOrEmpty(kstblPair.Item1) ? this._usingKeySpace : this.GetKeySpace(kstblPair.Item1);
            var ksInstance = string.IsNullOrEmpty(kstriggerPair.Item1) ? ksTblInstance : this.GetKeySpace(kstriggerPair.Item1);

            if (ksTblInstance == null) throw new ArgumentNullException("TableKeyspace", string.Format("CQL Trigger \"{0}\" for Table \"{1}\" could not find associated keyspace \"{2}\". DDL: {3}",
                                                                                                         name,
                                                                                                         tablename,
                                                                                                         kstblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                         triggerMatch.Groups[0].Value));
            if (ksInstance == null) throw new ArgumentNullException("Keyspace", string.Format("CQL Trigger \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                 name,
                                                                                                 kstriggerPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                 triggerMatch.Groups[0].Value));

            var tblInstance = ksTblInstance.TryGetTable(kstblPair.Item2);

            if (tblInstance == null) throw new ArgumentNullException("Table", string.Format("CQL Trigger \"{0}\" could not find associated Table \"{1}\" in Keyspace \"{2}\". DDL: {3}",
                                                                                             name,
                                                                                             tablename,
                                                                                             ksInstance.Name,
                                                                                             triggerMatch.Groups[0].Value));


            if (ksInstance.TryGetTrigger(kstriggerPair.Item2) == null)
            {
                var cqlTrigger = new CQLTrigger(this.File,
                                                lineNbr,
                                                kstriggerPair.Item2,
                                                tblInstance,
                                                triggerMatch.Groups[3].Value.Trim(),
                                                triggerMatch.Value,
                                                this.Node);

                this._ddlList.Add(cqlTrigger);

                return true;
            }

            return false;
        }

        private bool ProcessDDLFunction(Match functionMatch, uint lineNbr)
        {
            var name = functionMatch.Groups[1].Value.Trim();
            var ksfunctionPair = StringHelpers.SplitTableName(name, null);            
            var ksInstance = string.IsNullOrEmpty(ksfunctionPair.Item1) ? this._usingKeySpace : this.GetKeySpace(ksfunctionPair.Item1);

            if (ksInstance == null) throw new ArgumentNullException("Keyspace", string.Format("CQL Function \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                 name,
                                                                                                 ksfunctionPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                 functionMatch.Groups[0].Value));
            

            if (ksInstance.TryGetFunction(ksfunctionPair.Item2) == null)
            {
                var strColumns = functionMatch.Groups[2].Success ? functionMatch.Groups[2].Value.Trim() : null;

                if (string.IsNullOrEmpty(strColumns)) throw new ArgumentNullException("input parameters", string.Format("CQL Function \"{0}\" did not have any input parameters. DDL: {1}",
                                                                                                                            name,
                                                                                                                            functionMatch.Groups[0].Value));

                var columnsSplit = Common.StringFunctions.Split(strColumns,
                                                                    ',',
                                                                    StringFunctions.IgnoreWithinDelimiterFlag.AngleBracket
                                                                        | StringFunctions.IgnoreWithinDelimiterFlag.Parenthese,
                                                                    StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
                var columns = this.ProcessTableColumns(columnsSplit, ksInstance);
                var strReturnType = functionMatch.Groups[4].Success ? functionMatch.Groups[4].Value.Trim() : null;

                if (string.IsNullOrEmpty(strReturnType)) throw new ArgumentNullException("return type", string.Format("CQL Function \"{0}\" did not have a return type. DDL: {1}",
                                                                                                                            name,
                                                                                                                            functionMatch.Groups[0].Value));

                var returnType = this.ProcessColumnType(strReturnType, ksInstance);

                var cqlFunction = new CQLFunction(this.File,
                                                    lineNbr,
                                                    ksfunctionPair.Item2,
                                                    ksInstance,
                                                    functionMatch.Groups[3].Value.Trim(), //Call type
                                                    columns,
                                                    returnType,
                                                    functionMatch.Groups[5].Value.Trim(), //lang
                                                    functionMatch.Groups[6].Value.Trim(), //Code block
                                                    functionMatch.Value,
                                                    this.Node);

                this._ddlList.Add(cqlFunction);

                return true;
            }

            return false;
        }


        private IEnumerable<CQLColumn> ProcessTableColumns(List<string> columnSplits, IKeyspace keyspace)
        {
            var primarykeyCol = columnSplits.Last().StartsWith("primary ", StringComparison.OrdinalIgnoreCase);
            var endLength = primarykeyCol ? columnSplits.Count - 1 : columnSplits.Count;
            var columns = new List<CQLColumn>();

            for (int nIdx = 0; nIdx < endLength; ++nIdx)
            {
                columns.Add(ProcessTableColumn(columnSplits[nIdx], keyspace));
            }

            if (primarykeyCol)
            {
                string ignore;
                List<string> pkValues = new List<string>();

                Common.StringFunctions.ParseIntoFuncationParams(columnSplits.Last().Replace(" ", string.Empty), out ignore, out pkValues);

                if (pkValues[0][0] == '(')
                {
                    var pkCols = pkValues[0].Substring(1, pkValues[0].Length - 2).Split(',');

                    foreach (var pkCol in pkCols)
                    {
                        columns.ForEach(c => { if (c.Name == StringHelpers.RemoveQuotes(pkCol, false)) c.IsPrimaryKey = true; });
                    }
                }
                else
                {
                    columns.ForEach(c => { if (c.Name == StringHelpers.RemoveQuotes(pkValues[0],false)) c.IsPrimaryKey = true; });
                }

                foreach (var clusterCol in pkValues.Skip(1))
                {
                    columns.ForEach(c => { if (c.Name == StringHelpers.RemoveQuotes(clusterCol,false)) c.IsClusteringKey = true; });
                }
            }

            return columns;
        }

        private CQLColumn ProcessTableColumn(string column, IKeyspace keyspace)
        {
            var columntype = Common.StringFunctions.Split(column,
                                                            ' ',
                                                            StringFunctions.IgnoreWithinDelimiterFlag.AngleBracket
                                                                | StringFunctions.IgnoreWithinDelimiterFlag.Parenthese,
                                                            StringFunctions.SplitBehaviorOptions.StringTrimEachElement
                                                                | StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

            string colName = columntype[0];
            string colType = columntype[1];
            int colKeywordPos = 2;

            // col, coltype
            // col, coltype, static
            // col, coltype, primary, key
            // col, coltype, primary, key, static
            // col, coltype<subtype>
            // col, coltype, <subtype>
            if (columntype.Count > colKeywordPos)
            {
                if (columntype[colKeywordPos].First() == '<')
                {
                    colType += columntype[colKeywordPos];
                    ++colKeywordPos;
                }
            }

            return new CQLColumn(colName,
                                    this.ProcessColumnType(colType, keyspace),
                                    column);
        }

        private CQLColumnType ProcessColumnType(string colType, IKeyspace keyspace)
        {
            bool hasSubTypesd = colType.Last() == '>';
            IEnumerable<CQLColumnType> subTypes = null;

            if (hasSubTypesd)
            {
                string typeName;
                var typeParams = new List<string>();

                Common.StringFunctions.ParseIntoGenericTypeParams(colType.Replace(" ", string.Empty), out typeName, out typeParams);

                subTypes = typeParams.Select(i => ProcessColumnType(i, keyspace));

                return new CQLColumnType(typeName.ToLower(), subTypes, colType);
            }

            var udtInstance = CQLUserDefinedType.TryGet(keyspace, colType);
            bool isUDT = false;

            if (udtInstance != null)
            {
                var udtTypes = new List<CQLColumnType>();

                ProcessUDTColumn(udtInstance, udtTypes);
                subTypes = udtTypes;
                isUDT = true;
            }

            return new CQLColumnType(isUDT ? colType : colType.ToLower(), subTypes, colType, null, isUDT);
        }

        private void ProcessUDTColumn(ICQLUserDefinedType udtInstance, List<CQLColumnType> subTypes)
        {
            foreach (var column in udtInstance.Columns)
            {
                if (column is CQLColumn)
                {
                    subTypes.Add(((CQLColumn)column).CQLType);
                }
                else
                {
                    ProcessUDTColumn((ICQLUserDefinedType)udtInstance, subTypes);
                }
            }
        }

        private bool ProcessWithOptions(string strWithClause, IEnumerable<CQLColumn> columns, List<CQLOrderByColumn> orderByList, Dictionary<string, object> properties)
        {
            bool bResult = false;

            if (!string.IsNullOrEmpty(strWithClause))
            {
                Match withMatchItem = WithOrderByRegEx.Match(strWithClause);

                if (withMatchItem.Success)
                {
                    var strOrderByCols = withMatchItem.Groups[1].Value;

                    properties.Add("clustering order by", strOrderByCols);

                    var orderByClauses = strOrderByCols.Split(',').Select(n => StringHelpers.RemoveQuotes(n,false));

                    foreach (var orderByClause in orderByClauses)
                    {
                        var coldirSplit = orderByClause.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                        var colInstance = columns.FirstOrDefault(c => c.Name == coldirSplit[0]);

                        colInstance.IsInOrderBy = true;
                        orderByList.Add(new CQLOrderByColumn() { Column = colInstance, IsDescending = coldirSplit.Length > 1 ? coldirSplit[0].ToLower() == "desc" : false });
                    }
                    bResult = true;
                }

                withMatchItem = WithCompactStorageRegEx.Match(strWithClause);

                if (withMatchItem.Success)
                {
                    properties.Add("compact storage", true);
                    bResult = true;
                }

                var propertySplit = Common.StringFunctions.Split(strWithClause, " and ",
                                                                    StringComparison.CurrentCultureIgnoreCase,
                                                                    StringFunctions.IgnoreWithinDelimiterFlag.Brace
                                                                        | StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                                    StringFunctions.SplitBehaviorOptions.StringTrimEachElement
                                                                        | StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries
                                                                    );
                foreach (var propertyvalue in propertySplit)
                {
                    var propvalueSplitPos = propertyvalue.IndexOf('=');

                    if (propvalueSplitPos > 0)
                    {
                        var propName = propertyvalue.Substring(0, propvalueSplitPos).Trim().ToLower();
                        var propValue = propertyvalue.Substring(propvalueSplitPos + 1).Trim();

                        if(propValue.Last() == ';')
                        {
                            propValue = propValue.TrimEnd(';');
                        }

                        if (propValue[0] == '{')
                        {
                            properties.Add(propName, JsonConvert.DeserializeObject<Dictionary<string, object>>(propValue));
                        }
                        else
                        {
                            properties.Add(propName, StringHelpers.DetermineProperObjectFormat(propValue, true, false, false, propName));
                        }
                        bResult = true;
                    }
                }
            }

            return bResult;
        }
    }
}
