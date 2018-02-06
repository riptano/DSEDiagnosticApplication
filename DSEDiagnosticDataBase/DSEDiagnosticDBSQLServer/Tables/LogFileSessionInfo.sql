CREATE TABLE [DSEDiagnostic].[LogFileSessionInfo] (
    [NodeSessionId]      BIGINT             NULL,
    [Sequence]           TINYINT            NOT NULL,
    [NodeTimezone]       VARCHAR (15)       NULL,
    [TimezoneOffset]     TINYINT            NOT NULL,
    [LogTimeMin]         DATETIMEOFFSET (4) NOT NULL,
    [LogTimeMax]         DATETIMEOFFSET (4) NOT NULL,
    [LocalLogTimeMin]    DATETIME2 (4)      NOT NULL,
    [LocalLogTimeMax]    DATETIME2 (4)      NULL,
    [LogDuration_SEC]    BIGINT             NOT NULL,
    [LogGapDuration_SEC] INT                NOT NULL,
    [FileSize_MB]        DECIMAL (9, 4)     NOT NULL,
    [NbrEvents]          INT                NULL,
    [ExceptionIndicator] VARCHAR (50)       NULL,
    [LogFile]            VARCHAR (2089)     NOT NULL,
    [IsDebugLog]         BIT                CONSTRAINT [defo_LogFileSessionInfo_IsDebugLog] DEFAULT ((0)) NOT NULL,
    CONSTRAINT [fk_logfilesessioninfo] FOREIGN KEY ([NodeSessionId]) REFERENCES [DSEDiagnostic].[NodeSessionInfo] ([NodeSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [CUIdx_LogFileSessionInfo_RunId_NodeId_Sequence] UNIQUE NONCLUSTERED ([NodeSessionId] ASC, [Sequence] ASC)
);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Log file run session information used for the analysis.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'LogFileSessionInfo';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'The order of log files from a time sequence for a node. 0 is the eariest logs.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'LogFileSessionInfo', @level2type = N'COLUMN', @level2name = N'Sequence';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Tags log file if it is a duplicate, if there a gap from the previous log file, if the series mets min log duration threshold, etc.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'LogFileSessionInfo', @level2type = N'COLUMN', @level2name = N'ExceptionIndicator';

