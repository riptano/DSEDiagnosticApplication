<Query Kind="Program">
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Common.Functions.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Common.Functions.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Common.Path.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Common.Path.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Common.Patterns.Collections.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Common.Patterns.Collections.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Common.Patterns.QueueProcessor.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Common.Patterns.QueueProcessor.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Common.Patterns.Shared.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Common.Patterns.Shared.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Common.Patterns.Singleton.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Common.Patterns.Singleton.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Common.Patterns.Tasks.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Common.Patterns.Tasks.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Common.Patterns.Threading.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Common.Patterns.Threading.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Common.Patterns.TimeZoneInfo.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Common.Patterns.TimeZoneInfo.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Db4objects.Db4o.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Db4objects.Db4o.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Db4objects.Db4o.Linq.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Db4objects.Db4o.Linq.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\DSEDiagnosticFileParser.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\DSEDiagnosticFileParser.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\DSEDiagnosticLibrary.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\DSEDiagnosticLibrary.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\log4net.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\log4net.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Mono.Reflection.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Mono.Reflection.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\Newtonsoft.Json.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\Newtonsoft.Json.dll</Reference>
  <Reference Relative="DSEDiagnosticFileParser\bin\Debug\SevenZipSharp.dll">E:\Libraries\Projects\DataStax\Git\DSEDiagnosticApplication\DSEDiagnosticFileParser\bin\Debug\SevenZipSharp.dll</Reference>
  <Namespace>Common</Namespace>
  <Namespace>DSEDiagnosticFileParser</Namespace>
  <Namespace>DSEDiagnosticLibrary</Namespace>
  <Namespace>Newtonsoft.Json</Namespace>
</Query>

void Main()
{
	var mappers = JsonConvert.DeserializeObject<FileMapper[]>(
	"[" +
	"{\"Catagory\": 2, \"FilePatterns\": [\".\\\\nodes\\\\*\\\\logs\\\\cassandra\\\\system.log\"], \"FileParsingClass\": \"UserQuery.TestClass\", \"NodeIdPos\": -1}," +
	"{\"Catagory\": 5, \"FilePatterns\": [\".\\\\opscenterd\\\\node_info.json\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.json_node_info\", \"NodeIdPos\": -1, \"ProcessingTaskOption\":\"AllNodesInDataCenter,OnlyOnce\",\"ProcessPriorityLevel\":900}," +
	"{\"Catagory\": 4, \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nodetool\\\\status\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_nodetool_status\", \"NodeIdPos\": 0, \"ProcessingTaskOption\":136, \"ProcessPriorityLevel\":1000}," +
	"{\"Catagory\": 4, \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nodetool\\\\test\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_nodetool_status\", \"NodeIdPos\": 0, \"ProcessingTaskOption\":16, \"ProcessPriorityLevel\":925}," +
	"{\"Catagory\": 4, \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nodetool\\\\test1\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_nodetool_status\", \"NodeIdPos\": 0, \"ProcessingTaskOption\":16, \"ProcessPriorityLevel\":925}," +
	"{\"Catagory\": 4, \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nodetool\\\\test2\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_nodetool_status\", \"NodeIdPos\": 0, \"ProcessingTaskOption\":0, \"ProcessPriorityLevel\":925}," +
	"{\"Catagory\": 4, \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nodetool\\\\test3\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_nodetool_status\", \"NodeIdPos\": 0, \"ProcessingTaskOption\":16, \"ProcessPriorityLevel\":925}," +
	"{\"Catagory\": 4, \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nodetool\\\\test6\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_nodetool_status\", \"NodeIdPos\": 0, \"ProcessingTaskOption\":256, \"ProcessPriorityLevel\":925}," +
	"{\"Catagory\": 4, \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nodetool\\\\test4\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_nodetool_status\", \"NodeIdPos\": 0, \"ProcessingTaskOption\":0, \"ProcessPriorityLevel\":925}," +
	"{\"Catagory\": 4, \"FilePatterns\": [\".\\\\nodes\\\\*\\\\nodetool\\\\test5\"], \"FileParsingClass\": \"DSEDiagnosticFileParser.file_nodetool_status\", \"NodeIdPos\": 0, \"ProcessingTaskOption\":0, \"ProcessPriorityLevel\":925}" +
	"]")
							.OrderByDescending(o => o.ProcessPriorityLevel)
							.ThenBy(o => DSEDiagnosticFileParser.FileMapper.DetermineParallelOptions(o.ProcessingTaskOption))
							.GroupBy(k => new { ProcessPriorityLevel = k.ProcessPriorityLevel, ParallelProcessingWithinPriorityLevel = DSEDiagnosticFileParser.FileMapper.DetermineParallelOptions(k.ProcessingTaskOption)});

	mappers.Dump();
}

// Define other methods and classes here