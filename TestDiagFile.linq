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
  <Namespace>Common.Path</Namespace>
  <Namespace>DSEDiagnosticFileParser</Namespace>
  <Namespace>DSEDiagnosticLibrary</Namespace>
</Query>

void Main()
{
	var diagPath = PathUtils.BuildDirectoryPath(@"C:\Users\Richard\Desktop\Diag-Customer\Y31169_cluster-diagnostics-2017_01_06_08_02_04_UTC");

	var tasks = DSEDiagnosticFileParser.DiagnosticFile.ProcessFile(diagPath);
	
	tasks.Wait();
	tasks.Result.Dump(1);
	
	DSEDiagnosticLibrary.Cluster.CurrentCluster.Dump();

}

// Define other methods and classes here
