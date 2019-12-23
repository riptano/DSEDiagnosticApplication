sudo nuget restore DSEDiagnosticApplication.Core.sln
dotnet msbuild DSEDiagnosticApplication.Core.sln /t:Restore /p:Configuration=Release-NoRepro
