CREATE TABLE [DSEDiagnostic].[DDLObjectProperties] (
    [DDLSessionId]  BIGINT        NOT NULL,
    [PropertyName]  VARCHAR (100) NOT NULL,
    [PropertyValue] VARCHAR (250) NOT NULL,
    CONSTRAINT [fk_ddlobjectproperties] FOREIGN KEY ([DDLSessionId]) REFERENCES [DSEDiagnostic].[DDLObjectsSessionInfo] ([DDLSessionId]) ON DELETE CASCADE ON UPDATE CASCADE,
    CONSTRAINT [CUIdx_DDLObjectProperties_Id_Name] UNIQUE NONCLUSTERED ([DDLSessionId] ASC, [PropertyName] ASC)
);


GO
EXECUTE sp_addextendedproperty @name = N'MS_Description', @value = N'Additioinal Properties assocated to the referenced DDL object for a session. For example this would be the "with" properties of a table.', @level0type = N'SCHEMA', @level0name = N'DSEDiagnostic', @level1type = N'TABLE', @level1name = N'DDLObjectProperties';

