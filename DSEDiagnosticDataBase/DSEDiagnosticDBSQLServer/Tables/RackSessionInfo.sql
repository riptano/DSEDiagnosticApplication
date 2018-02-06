CREATE TABLE [DSEDiagnostic].[RackSessionInfo] (
    [RackSessionId]       BIGINT       IDENTITY (1, 1) NOT NULL,
    [DataCenterSessionId] BIGINT       NOT NULL,
    [Name]                VARCHAR (25) NOT NULL,
    [NbrNodes]            SMALLINT     CONSTRAINT [defo_RackSessionInfo_NbrNodes] DEFAULT ((0)) NOT NULL,
    [DCSuffix]            VARCHAR (25) NULL,
    [PreferLocal]         BIT          NULL,
    [IsDefault]           BIT          NULL,
    CONSTRAINT [fk_racksessioninfo] FOREIGN KEY ([DataCenterSessionId]) REFERENCES [DSEDiagnostic].[DataCenterSessionInfo] ([DataCenterSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [CUI_RackSessionInfo_RunSess_DCId_Name] UNIQUE NONCLUSTERED ([DataCenterSessionId] ASC, [Name] ASC),
    CONSTRAINT [Unq_RackSessionInfo_RackSessionId] UNIQUE NONCLUSTERED ([RackSessionId] ASC)
);


GO
CREATE NONCLUSTERED INDEX [Idx_RackSessionInfo_DataCenterSessionId]
    ON [DSEDiagnostic].[RackSessionInfo]([DataCenterSessionId] ASC);

