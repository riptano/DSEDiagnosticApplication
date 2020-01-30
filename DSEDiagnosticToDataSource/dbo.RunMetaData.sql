CREATE TABLE [dbo].[RunMetaData]
(
	[DiagnosticId]			VARCHAR (115) NOT NULL, 
	[Cluster Name]			VARCHAR (100) NOT NULL,
	[RunTimestamp]			DATETIMEOFFSET  NOT NULL,
	[ClusterId]				UNIQUEIDENTIFIER NULL,
	[InsightClusterId]		UNIQUEIDENTIFIER NULL,
	[AnalysisPeriodStart]	DATETIMEOFFSET NULL,
	[AnalysisPeriodEnd]		DATETIMEOFFSET NULL,
	[AggregationDuration]	BIGINT NOT NULL,
	[NbrDCs]				INT NOT NULL,
	[NbrNodes]				INT NOT NULL,
	[NbrKeyspaces]			INT NOT NULL,
	[NbrTables]				INT NOT NULL,
	[Diagnostic Data Quality] VARCHAR (50) NULL,
	[Aborted]				BIT NULL,
	[RunExceptions]			INT NOT NULL,
	[RunErrors]				INT NOT NULL,
	[RunWarnings]			INT NOT NULL,
	[Version]				VARCHAR(100) NULL,
	[CommandLine]			VARCHAR(MAX),
    CONSTRAINT [PK_Table] PRIMARY KEY ([DiagnosticId])
)
