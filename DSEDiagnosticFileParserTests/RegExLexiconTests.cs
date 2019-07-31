using Microsoft.VisualStudio.TestTools.UnitTesting;
using DSEDiagnosticFileParser;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticFileParser.Tests
{
    [TestClass()]
    public class RegExLexiconTests
    {
        readonly string[] TstStrs = new string[]
                                    {
                                        "^Flushing\\s+SecondaryIndex\\s+(?<IndexClass>[^ (,{]+)\\s*\\{\\s*(?<KEYITEM>columnDefs)\\s*=\\s*\\[\\s*(?:ColumnDefinition\\s*\\{\\s*(?:(?:(?<ITEM1>indexName)\\s*\\=\\s*(?<SOLRINDEXNAME>(?<ITEM2>[^},{]+))|(?<ITEM1>[^={]+)\\s*\\=\\s*(?<ITEM2>[^},{]+))(?:\\,|\\})\\s*)+\\s*\\,?\\s*)+",
                                        "^Executing\\s+hard\\s+commit\\s+on\\s+index\\s+(?$<DDLITEM>)$",
                                        "^commitInternalComplete\\s+duration\\s*=\\s*(?$<DURATION>\\s*\\w*)\\s+startTime\\s*=\\s*(?<CommitStart>[0-9,\\-]+)$",
                                        "^updating shards[a-z ]+/?\\s*(?$<NODE>)\\s+\\w+\\s\\w+\\s(?<status>schema)=?(?$<DDLSCHEMAID>)?$",
                                        "^(?<action>Create) new\\s+(?<type>table|columnfamily)\\s*:\\s+org.apache.cassandra.config.CFMetaData@[0-9a-f]+\\[cfid=(?$<DDLSCHEMAID>),ksname=(?<KEYSPACE>[a-z0-9'\\-_$%+=!?<>^*&@]+),cfname=(?<DDLITEMNAME>$<!DDLITEMNAME>)",
                                        "^(?<action>Create) new\\s+(?<type>view)\\s*:\\s+org.apache.cassandra.config.ViewDefinition@[0-9a-f]+\\[ksname=(?$<KEYSPACE>),viewname=(?$<DDLITEMNAME>),basetableid=(?<basetaleid>[a-f0-9\\-]+),baseTableName=(?<basetablename>[a-z0-9'\\-_$%+=!?<>^*&@]+),.+cfid=(?$<DDLSCHEMAID>),",
                                        "^Compacted\\s(?<sstables>\\d+)\\ssstables\\s+to\\s*\\[(?$<SSTABLEPATHS>)\\]\\.\\s+(?<size>[0-9.,]+\\s+[a-z]+)\\s+to\\s+(?<newsize__size>[0-9.,]+)\\s\\([0-9a-z\\ %~,.]+\\)+\\sin\\s+(?$<DURATION>\\s*[a-z/]+)\\s*\\=\\s*(?<iorate>[0-9.,]+\\s*[a-z/]+)\\.\\s+(?<totalpartions>[0-9,]+)\\s+total\\s+partitions\\s+merged\\s+to\\s+(?<mergedpartitions>[0-9,]+)\\.\\s+Partition\\s+merge\\s+counts\\s+were\\s+\\{(?<mergecounts>[0-9:\\ ,]+)\\s*\\}$",
                                        "^updating shards[a-z ]+/?\\s*(?$<NODE>)\\s+\\w+\\s\\w+\\s(?<status>schema)=$<!DDLSCHEMAID>$",
                                    };

        readonly string[] TstStrsResults = new string[]
                                    {
                                        "^Flushing\\s+SecondaryIndex\\s+(?<IndexClass>[^ (,{]+)\\s*\\{\\s*(?<KEYITEM>columnDefs)\\s*=\\s*\\[\\s*(?:ColumnDefinition\\s*\\{\\s*(?:(?:(?<ITEM1>indexName)\\s*\\=\\s*(?<SOLRINDEXNAME>(?<ITEM2>[^},{]+))|(?<ITEM1>[^={]+)\\s*\\=\\s*(?<ITEM2>[^},{]+))(?:\\,|\\})\\s*)+\\s*\\,?\\s*)+",
                                        "^Executing\\s+hard\\s+commit\\s+on\\s+index\\s+(?<DDLITEM>[^ ]+)$",
                                        "^commitInternalComplete\\s+duration\\s*=\\s*(?<DURATION>[0-9,.]+\\s*\\w*)\\s+startTime\\s*=\\s*(?<CommitStart>[0-9,\\-]+)$",
                                        "^updating shards[a-z ]+/?\\s*(?<NODE>[^ ]+)\\s+\\w+\\s\\w+\\s(?<status>schema)=?(?<DDLSCHEMAID>[0-9a-f\\-]+)?$",
                                        "^(?<action>Create) new\\s+(?<type>table|columnfamily)\\s*:\\s+org.apache.cassandra.config.CFMetaData@[0-9a-f]+\\[cfid=(?<DDLSCHEMAID>[0-9a-f\\-]+),ksname=(?<KEYSPACE>[a-z0-9'\\-_$%+=!?<>^*&@]+),cfname=(?<DDLITEMNAME>[a-z0-9'\\-_$%+=!?<>^*&@]+)",
                                        "^(?<action>Create) new\\s+(?<type>view)\\s*:\\s+org.apache.cassandra.config.ViewDefinition@[0-9a-f]+\\[ksname=(?<KEYSPACE>[a-z0-9'\\-_$%+=!?<>^*&@]+),viewname=(?<DDLITEMNAME>[a-z0-9'\\-_$%+=!?<>^*&@]+),basetableid=(?<basetaleid>[a-f0-9\\-]+),baseTableName=(?<basetablename>[a-z0-9'\\-_$%+=!?<>^*&@]+),.+cfid=(?<DDLSCHEMAID>[0-9a-f\\-]+),",
                                        "^Compacted\\s(?<sstables>\\d+)\\ssstables\\s+to\\s*\\[(?<SSTABLEPATHS>[a-z0-9\\-_@#/.,\\ +%]*)\\]\\.\\s+(?<size>[0-9.,]+\\s+[a-z]+)\\s+to\\s+(?<newsize__size>[0-9.,]+)\\s\\([0-9a-z\\ %~,.]+\\)+\\sin\\s+(?<DURATION>[0-9,.]+\\s*[a-z/]+)\\s*\\=\\s*(?<iorate>[0-9.,]+\\s*[a-z/]+)\\.\\s+(?<totalpartions>[0-9,]+)\\s+total\\s+partitions\\s+merged\\s+to\\s+(?<mergedpartitions>[0-9,]+)\\.\\s+Partition\\s+merge\\s+counts\\s+were\\s+\\{(?<mergecounts>[0-9:\\ ,]+)\\s*\\}$",
                                        "^updating shards[a-z ]+/?\\s*(?<NODE>[^ ]+)\\s+\\w+\\s\\w+\\s(?<status>schema)=[0-9a-f\\-]+$",
                                    };

        [TestMethod()]
        public void FindReplaceRegExTest()
        {
            var RegExLexiconValues = new RegExLexicon(DSEDiagnosticParamsSettings.Helpers.ReadJsonFileIntoObject<KeyValuePair<string, string>[]>(DSEDiagnosticFileParserTests.Properties.Settings.Default.test));

            for (int nIdx = 0; nIdx < TstStrs.Length; ++nIdx)
            {
                Assert.AreEqual(TstStrsResults[nIdx],
                                RegExLexiconValues.FindReplaceRegEx(TstStrs[nIdx]));
            }
        }
    }
}