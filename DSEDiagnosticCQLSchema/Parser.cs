using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Text.RegularExpressions;
using Common;
using Newtonsoft.Json;
using DSEDiagnosticLibrary;
using DSEDiagnosticLogger;

namespace DSEDiagnosticCQLSchema
{
    public static class Parser
    {
        public static bool ProcessDDLTable(IKeyspace ksInstance,
                                            string name,
                                            IList<string> columnsSplit,
                                            string withClause,
                                            string ddl,
                                            IFilePath filePath,
                                            INode node,
                                            uint lineNbr,
                                            out CQLTable cqlTable,
                                            bool checkIfExist = true)
        {
           var kstblPair = StringHelpers.SplitTableName(name, null);

            cqlTable = null;

            if (ksInstance == null) throw new ArgumentNullException("keyspace", string.Format("CQL Table \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                 kstblPair.Item2,
                                                                                                 kstblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                 ddl));

            if (!checkIfExist || ksInstance.TryGetTable(kstblPair.Item2) == null)
            {
                var columns = ProcessTableColumns(columnsSplit, ksInstance);
                var properties = new Dictionary<string, object>();
                var orderByList = new List<CQLOrderByColumn>();

                ProcessWithOptions(withClause, columns, orderByList, properties);

                cqlTable = new CQLTable(filePath,
                                            lineNbr,
                                            ksInstance,
                                            kstblPair.Item2,
                                            columns,
                                            null,
                                            null,
                                            orderByList,
                                            properties,
                                            ddl,
                                            node);                
                return true;
            }

            return false;
        }

        public static bool ProcessDDLUDT(IKeyspace ksInstance,
                                            string name,
                                            IList<string> columnsSplit,
                                            string ddl,
                                            IFilePath filePath,
                                            INode node,
                                            uint lineNbr,
                                            out CQLUserDefinedType cqlUDT,
                                            bool checkIfExist = true)
        {
            var kstblPair = StringHelpers.SplitTableName(name, null);

            cqlUDT = null;

            if (ksInstance == null) throw new ArgumentNullException("keyspace", string.Format("CQL Type (UDT) \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                 kstblPair.Item2,
                                                                                                 kstblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                 ddl));

            if (!checkIfExist || ksInstance.TryGetUDT(kstblPair.Item2) == null)
            {               
                var columns = ProcessTableColumns(columnsSplit, ksInstance);

                cqlUDT = new CQLUserDefinedType(filePath,
                                                    lineNbr,
                                                    ksInstance,
                                                    kstblPair.Item2,
                                                    columns,
                                                    ddl,
                                                    node);                
                return true;
            }

            return false;
        }

        public static bool ProcessDDLIndex(IKeyspace ksInstance,
                                            string name,
                                            IKeyspace ksTblInstance,
                                            string tableName,
                                            IList<string> columnsSplit,
                                            string usingClass,
                                            string withOptions,
                                            bool isCustomIdx,
                                            string ddl,
                                            IFilePath filePath,
                                            INode node,
                                            uint lineNbr,
                                            out CQLIndex cqlIdx,
                                            bool checkIfExist = true)
        {
            var ksidxPair = StringHelpers.SplitTableName(name, null);            
            var kstblPair = StringHelpers.SplitTableName(tableName, null);

            cqlIdx = null;

            if (ksTblInstance == null) throw new ArgumentNullException("TableKeyspace", string.Format("CQL Index \"{0}\" for Table \"{1}\" could not find associated keyspace \"{2}\". DDL: {3}",
                                                                                                         name,
                                                                                                         tableName,
                                                                                                         kstblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                         ddl));
            if (ksInstance == null) throw new ArgumentNullException("Keyspace", string.Format("CQL Index \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                 name,
                                                                                                 ksidxPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                 ddl));

            var tblInstance = ksTblInstance.TryGetTable(kstblPair.Item2);

            if (tblInstance == null) throw new ArgumentNullException("Table", string.Format("CQL Index \"{0}\" could not find associated Table \"{1}\" in Keyspace \"{2}\". DDL: {3}",
                                                                                             name,
                                                                                             tableName,
                                                                                             ksInstance.Name,
                                                                                             ddl));


            if (!checkIfExist || ksInstance.TryGetIndex(ksidxPair.Item2) == null)
            {                
                var colFunctions = columnsSplit.Select(i =>
                {
                    List<string> columns = new List<string>();
                    string functionName = null;

                    if (i.Last() == ')')
                    {
                        Common.StringFunctions.ParseIntoFuncationParams(i, out functionName, out columns);
                    }
                    else
                    {
                        columns.Add(i);
                    }

                    return new CQLFunctionColumn(functionName, tblInstance.TryGetColumns(columns), i);
                });
               
                cqlIdx = new CQLIndex(filePath,
                                        lineNbr,
                                        ksidxPair.Item2,
                                        tblInstance,
                                        colFunctions,
                                        isCustomIdx,
                                        usingClass,
                                        string.IsNullOrEmpty(withOptions)
                                            ? null
                                            : JsonConvert.DeserializeObject<Dictionary<string, object>>(withOptions),
                                        ddl,
                                        node);
                
                return true;
            }

            return false;
        }

        public static bool ProcessDDLMaterializediew(IKeyspace viewKSInstance,
                                                        string viewName,
                                                        IKeyspace tblKSInstance,
                                                        string tblName,
                                                        IList<string> columnsSplit,
                                                        string strPrimaryKeys,
                                                        string strWithClause,
                                                        string strWhereClause,                                                       
                                                        string ddl,
                                                        IFilePath filePath,
                                                        INode node,
                                                        uint lineNbr,
                                                        out CQLMaterializedView cqlView,
                                                        bool checkIfExist = true)
        {            
            var viewKsTblPair = StringHelpers.SplitTableName(viewName, null);            
            var tblKsTblPair = StringHelpers.SplitTableName(tblName, null);

            cqlView = null;

            if (viewKSInstance == null) throw new ArgumentNullException("Keyspace", string.Format("CQL Materialized View \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                         viewKsTblPair.Item2,
                                                                                                         viewKsTblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                         ddl));

            if (tblKSInstance == null) throw new ArgumentNullException("TableKeyspace", string.Format("CQL Materialized View \"{0}\" could not find associated table keyspace \"{1}\". DDL: {2}",
                                                                                                        tblKsTblPair.Item2,
                                                                                                        tblKsTblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                        ddl));
            var tblInstance = tblKSInstance.TryGetTable(tblKsTblPair.Item2);

            if (tblInstance == null) throw new ArgumentNullException("Table", string.Format("CQL Materialized View \"{0}\" could not find associated Table \"{1}\" in Keyspace \"{2}\". DDL: {3}",
                                                                                             viewName,
                                                                                             tblKsTblPair.Item2,
                                                                                             tblKSInstance.Name,
                                                                                             ddl));

            if (!checkIfExist || viewKSInstance.TryGetView(viewKsTblPair.Item2) == null)
            {                
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

                cqlView = new CQLMaterializedView(filePath,
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
                                                    ddl,
                                                    node);
                
                return true;
            }

            return false;
        }

        public static bool ProcessDDLTrigger(IKeyspace ksInstance,
                                                string name,
                                                IKeyspace ksTblInstance,
                                                string tableName,
                                                string javaClass,
                                                string ddl,
                                                IFilePath filePath,
                                                INode node,
                                                uint lineNbr,
                                                out CQLTrigger cqlTrigger,
                                                bool checkIfExist = true)
        {
            var kstriggerPair = StringHelpers.SplitTableName(name, null);
            var kstblPair = StringHelpers.SplitTableName(tableName, null);

            cqlTrigger = null;

            if (ksTblInstance == null) throw new ArgumentNullException("TableKeyspace", string.Format("CQL Trigger \"{0}\" for Table \"{1}\" could not find associated keyspace \"{2}\". DDL: {3}",
                                                                                                         name,
                                                                                                         tableName,
                                                                                                         kstblPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                         ddl));
            if (ksInstance == null) throw new ArgumentNullException("Keyspace", string.Format("CQL Trigger \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                 name,
                                                                                                 kstriggerPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                 ddl));

            var tblInstance = ksTblInstance.TryGetTable(kstblPair.Item2);

            if (tblInstance == null) throw new ArgumentNullException("Table", string.Format("CQL Trigger \"{0}\" could not find associated Table \"{1}\" in Keyspace \"{2}\". DDL: {3}",
                                                                                             name,
                                                                                             tableName,
                                                                                             ksInstance.Name,
                                                                                             ddl));


            if (!checkIfExist || ksInstance.TryGetTrigger(kstriggerPair.Item2) == null)
            {
                cqlTrigger = new CQLTrigger(filePath,
                                                lineNbr,
                                                kstriggerPair.Item2,
                                                tblInstance,
                                                javaClass,
                                                ddl,
                                                node);
                
                return true;
            }

            return false;
        }

        public static bool ProcessDDLFunction(IKeyspace ksInstance,
                                                string name,
                                                IList<string> columnsSplit,
                                                string callTypeAction,
                                                string langague,
                                                string codeBlock,
                                                string strReturnType,
                                                string ddl,
                                                IFilePath filePath,
                                                INode node,
                                                uint lineNbr,
                                                out CQLFunction cqlFunction,
                                                bool checkIfExist = true)
        {
            var ksfunctionPair = StringHelpers.SplitTableName(name, null);

            cqlFunction = null;

            if (ksInstance == null) throw new ArgumentNullException("Keyspace", string.Format("CQL Function \"{0}\" could not find associated keyspace \"{1}\". DDL: {2}",
                                                                                                 name,
                                                                                                 ksfunctionPair.Item1 ?? "<CQL use stmt required or fully qualified name>",
                                                                                                 ddl));


            if (!checkIfExist || ksInstance.TryGetFunction(ksfunctionPair.Item2) == null)
            {                
                var columns = ProcessTableColumns(columnsSplit, ksInstance);
                
                if (string.IsNullOrEmpty(strReturnType)) throw new ArgumentNullException("return type", string.Format("CQL Function \"{0}\" did not have a return type. DDL: {1}",
                                                                                                                            name,
                                                                                                                            ddl));

                var returnType = ProcessColumnType(strReturnType, ksInstance);

                cqlFunction = new CQLFunction(filePath,
                                                lineNbr,
                                                ksfunctionPair.Item2,
                                                ksInstance,
                                                callTypeAction,
                                                columns,
                                                returnType,
                                                langague,
                                                codeBlock,
                                                ddl,
                                                node);
                
                return true;
            }

            return false;
        }


        private static IEnumerable<CQLColumn> ProcessTableColumns(IList<string> columnSplits, IKeyspace keyspace)
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
                List<string> pkValues = new List<string>();

                Common.StringFunctions.ParseIntoFuncationParams(columnSplits.Last().Replace(" ", string.Empty), out string ignore, out pkValues);

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
                    columns.ForEach(c => { if (c.Name == StringHelpers.RemoveQuotes(pkValues[0], false)) c.IsPrimaryKey = true; });
                }

                foreach (var clusterCol in pkValues.Skip(1))
                {
                    columns.ForEach(c => { if (c.Name == StringHelpers.RemoveQuotes(clusterCol, false)) c.IsClusteringKey = true; });
                }
            }

            return columns;
        }

        private static CQLColumn ProcessTableColumn(string column, IKeyspace keyspace)
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
                                    ProcessColumnType(colType, keyspace),
                                    column);
        }

        private static CQLColumnType ProcessColumnType(string colType, IKeyspace keyspace)
        {
            bool hasSubTypesd = colType.Last() == '>';
            IEnumerable<CQLColumnType> subTypes = null;
            string prefixKSName = null;

            if (hasSubTypesd)
            {
                var typeParams = new List<string>();

                Common.StringFunctions.ParseIntoGenericTypeParams(colType.Replace(" ", string.Empty), out string typeName, out typeParams);

                subTypes = typeParams.Select(i => ProcessColumnType(i, keyspace));

                return new CQLColumnType(typeName.ToLower(), subTypes, colType);
            }

            {
                var ksUDTPair = StringHelpers.SplitTableName(colType, null);

                if (!string.IsNullOrEmpty(ksUDTPair.Item1))
                {
                    if (keyspace.Name == ksUDTPair.Item1)
                    {
                        colType = ksUDTPair.Item2;
                    }
                    else
                    {
                        var newKS = keyspace.DataCenter.TryGetKeyspace(ksUDTPair.Item1);

                        if (newKS != null)
                        {
                            colType = ksUDTPair.Item2;

                            if (!newKS.Equals(keyspace))
                            {
                                keyspace = newKS;
                                prefixKSName = keyspace.Name;
                            }

                        }
                    }
                }
            }

            var udtInstance = CQLUserDefinedType.TryGet(keyspace, colType);
            bool isUDT = false;

            if (udtInstance != null)
            {
                var udtTypes = new List<CQLColumnType>();

                ProcessUDTColumn(udtInstance, udtTypes);
                subTypes = udtTypes;
                isUDT = true;

                if (!string.IsNullOrEmpty(prefixKSName))
                    colType = prefixKSName + '.' + colType;
            }

            return new CQLColumnType(isUDT ? colType : colType.ToLower(), subTypes, colType, null, isUDT);
        }

        private static void ProcessUDTColumn(ICQLUserDefinedType udtInstance, List<CQLColumnType> subTypes)
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

        static readonly Regex WithCompactStorageRegEx = new Regex(@"(?:\s|^)compact\s+storage(?:\s|\;|$)", RegexOptions.Compiled | RegexOptions.IgnoreCase);
        static readonly Regex WithOrderByRegEx = new Regex(@"(?:\s|^)clustering\s+order\s+by\s*\(([a-z0-9\-_$%+=@!?^*&,\ ]+)\)(?:\s|\;|$)", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        private static bool ProcessWithOptions(string strWithClause,
                                                IEnumerable<CQLColumn> columns,
                                                List<CQLOrderByColumn> orderByList,
                                                Dictionary<string, object> properties)
        {
            bool bResult = false;

            if (!string.IsNullOrEmpty(strWithClause))
            {
                Match withMatchItem = WithOrderByRegEx.Match(strWithClause);

                if (withMatchItem.Success)
                {
                    var strOrderByCols = withMatchItem.Groups[1].Value;

                    properties.Add("clustering order by", strOrderByCols);

                    var orderByClauses = strOrderByCols.Split(',').Select(n => StringHelpers.RemoveQuotes(n, false));

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

                        if (propValue.Last() == ';')
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
