CREATE TABLE [DSEDiagnostic].[NodeCaches] (
    [NodeSessionId] BIGINT        NOT NULL,
    [Property]      VARCHAR (10)  NULL,
    [Value]         VARCHAR (100) NULL,
    CONSTRAINT [fk_nodecaches_nodesessioninfo] FOREIGN KEY ([NodeSessionId]) REFERENCES [DSEDiagnostic].[NodeSessionInfo] ([NodeSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [UCI_NodeCaches_NodeSess_Prop] UNIQUE NONCLUSTERED ([NodeSessionId] ASC, [Property] ASC)
);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Need to break out cache attributes', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'NodeCaches';


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'e.g., Key, Counter, Row', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'NodeCaches', @level2type = N'COLUMN', @level2name = N'Property';

