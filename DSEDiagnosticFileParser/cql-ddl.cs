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
            if (this.Node != null)
            {
                this.Cluster = this.Node.Cluster;
            }

            if (this.Cluster == null)
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
        //
        // RegEx: 3
        // CREATE TYPE [IF NOT EXISTS] [keyspace.]type_name ( field, field, ...)
        // null, [keyspace_name.]table_name, column_definition_clause, null
        //
        // RegEx: 4
        // CREATE [CUSTOM] INDEX [IF NOT EXISTS] index_name ON [keyspace_name.]table_name ( column_name, [KEYS ( column_name )] ) [USING class_name] [WITH OPTIONS = json-map]
        // null, [CUSTOM], index_name, [keyspace_name.]table_name, column_definition_clause, [class_name], [json-map], null
        //
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

                if (multipleLineComment)
                {
                    if (line.EndsWith(@"*/"))
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

                    if (startComment >= 0)
                    {
                        var endComment = line.Substring(startComment + 1).LastIndexOf(@"*/");

                        if (endComment < 0)
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

            if (cqlStatements.Count == 0)
            {
                this.Processed = false;
                return 0;
            }

            Match regexMatch;

            foreach (var cqlStmt in cqlStatements)
            {
                ++itemNbr;
                regexMatch = this.RegExParser.Match(cqlStmt, 0); //cql use

                if (regexMatch.Success)
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
                    if (ProcessDDLKeyspace(regexMatch, itemNbr))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }

                regexMatch = this.RegExParser.Match(cqlStmt, 2); //create table

                if (regexMatch.Success)
                {
                    if (ProcessDDLTable(regexMatch, itemNbr))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }

                regexMatch = this.RegExParser.Match(cqlStmt, 3); //create type

                if (regexMatch.Success)
                {
                    if (ProcessDDLTable(regexMatch, itemNbr))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }

                regexMatch = this.RegExParser.Match(cqlStmt, 4); //create index

                if (regexMatch.Success)
                {
                    if (ProcessDDLIndex(regexMatch, itemNbr))
                    {
                        ++nbrGenerated;
                    }
                    continue;
                }
            }

            this.NbrItemsParsed = (int)itemNbr;
            this.Processed = true;
            return nbrGenerated;
        }

        private IKeyspace GetKeySpace(string ksName)
        {
            var ksInstance = this._localKS.FirstOrDefault(k => k.Name == ksName);

            if (ksInstance == null)
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
            var durableWrites = keyspaceMatch.Groups[4].Success && keyspaceMatch.Groups[4].Value.ToUpper() == "DURABLE_WRITES"
                                        ? bool.Parse(keyspaceMatch.Groups[5].Value)
                                        : true;
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

        readonly Regex WithCompactStorageRegEx = new Regex(@"\s*compact\s+storage\s*", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        readonly Regex WithOrderByRegEx = new Regex(@"\s*clustering\s+order\s+by\s*\(([a-z0-9\-_$%+=@!?^*&,\ ]+)\)\s*", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private bool ProcessDDLTable(Match tableMatch, uint lineNbr)
        {
            var name = tableMatch.Groups[1].Value.Trim();
            var kstblPair = StringHelpers.SplitTableName(name, null);
            var ksInstance = string.IsNullOrEmpty(kstblPair.Item1) ? this._usingKeySpace : this.GetKeySpace(kstblPair.Item1);

            if (ksInstance == null) throw new NullReferenceException(string.Format("CQL Table \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                     kstblPair.Item2,
                                                                                     kstblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                     tableMatch.Groups[0].Value));

            if (ksInstance.TryGetTable(kstblPair.Item2) == null)
            {
                var strColumns = tableMatch.Groups[2].Value.Trim();
                var strWithClause = tableMatch.Groups[3].Success ? tableMatch.Groups[3].Value.Trim().TrimEnd(';') : null;
                var columnsSplit = Common.StringFunctions.Split(strColumns,
                                                                    ',',
                                                                    StringFunctions.IgnoreWithinDelimiterFlag.AngleBracket
                                                                        | StringFunctions.IgnoreWithinDelimiterFlag.Parenthese,
                                                                    StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
                var columns = this.ProcessTableColumns(columnsSplit, ksInstance);
                var properties = new Dictionary<string, object>();
                var orderByList = new List<CQLOrderByColumn>();

                #region With Clause
                if(!string.IsNullOrEmpty(strWithClause))
                {
                    Match withMatchItem = WithOrderByRegEx.Match(strWithClause);

                    if (withMatchItem.Success)
                    {
                        var strOrderByCols = withMatchItem.Groups[1].Value;

                        properties.Add("clustering order by", strOrderByCols);

                        var orderByClauses = strOrderByCols.Split(',');

                        foreach (var orderByClause in orderByClauses)
                        {
                            var coldirSplit = orderByClause.Split(new char[] { ' ' }, StringSplitOptions.RemoveEmptyEntries);
                            var colInstance = columns.FirstOrDefault(c => c.Name == coldirSplit[0]);

                            colInstance.IsInOrderBy = true;
                            orderByList.Add(new CQLOrderByColumn() { Column = colInstance, IsDescending = coldirSplit.Length > 1 ? coldirSplit[0].ToLower() == "desc" : false });
                        }
                    }

                    withMatchItem = WithCompactStorageRegEx.Match(strWithClause);

                    if (withMatchItem.Success)
                    {
                        properties.Add("compact storage", true);
                    }
                }

                {
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

                            if (propValue[0] == '{')
                            {
                                properties.Add(propName, JsonConvert.DeserializeObject<Dictionary<string, object>>(propValue));
                            }
                            else
                            {
                                properties.Add(propName, StringHelpers.DetermineProperObjectFormat(propValue, true, false));
                            }
                        }
                    }
                }
                #endregion

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

            if (ksInstance == null) throw new NullReferenceException(string.Format("CQL Type (UDT) \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
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
            var tablename = indexMatch.Groups[3].Value.Trim();
            var kstblPair = StringHelpers.SplitTableName(tablename, null);
            var ksInstance = string.IsNullOrEmpty(kstblPair.Item1) ? this._usingKeySpace : this.GetKeySpace(kstblPair.Item1);

            if (ksInstance == null) throw new NullReferenceException(string.Format("CQL Index \"{0}\" for Table \"{1}\" could not find associated keyspace \"{2}\". DDL: {3}",
                                                                                     name,
                                                                                     tablename,
                                                                                     kstblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                     indexMatch.Groups[0].Value));
            var tblInstance = ksInstance.TryGetTable(kstblPair.Item2);

            if (tblInstance == null) throw new NullReferenceException(string.Format("CQL Index \"{0}\" could not find associated Table \"{1}\" in Keyspace \"{2}\". DDL: {3}",
                                                                                     name,
                                                                                     tablename,
                                                                                     ksInstance.Name,
                                                                                     indexMatch.Groups[0].Value));


            if (ksInstance.TryGetIndex(name) == null)
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
                                    ? indexMatch.Groups[5].Value.Trim().TrimEnd(';')
                                    : null;
                var withOptions = indexMatch.Groups[6].Success
                                    ? indexMatch.Groups[6].Value.Trim().TrimEnd(';')
                                    : null;

                var cqlUDT = new CQLIndex(this.File,
                                            lineNbr,
                                            name,
                                            tblInstance,
                                            colFunctions,
                                            indexMatch.Groups[1].Success && indexMatch.Groups[1].Value.ToLower() == "custom",
                                            usingClass,
                                            string.IsNullOrEmpty(withOptions)
                                                ? null
                                                : JsonConvert.DeserializeObject<Dictionary<string,object>>(withOptions),
                                            indexMatch.Value,
                                            this.Node);

                this._ddlList.Add(cqlUDT);

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
                        columns.ForEach(c => { if (c.Name == pkCol) c.IsPrimaryKey = true; });
                    }
                }
                else
                {
                    columns.ForEach(c => { if (c.Name == pkValues[0]) c.IsPrimaryKey = true; });
                }

                foreach (var clusterCol in pkValues.Skip(1))
                {
                    columns.ForEach(c => { if (c.Name == clusterCol) c.IsClusteringKey = true; });
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
                                                            StringFunctions.SplitBehaviorOptions.StringTrimEachElement);

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
    }
}
