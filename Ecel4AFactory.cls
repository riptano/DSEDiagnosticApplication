VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "Factory"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = False
Attribute VB_Exposed = False
Option Explicit

Public Function MergePivotFieldsList(pageFields As Object, columnFields As Object, rowFields As Object) As Collection
    Dim pf As pivotField
    Dim pfi As PivotFieldrInfo
    
    Set MergePivotFieldsList = New Collection
    
    If Not pageFields Is Nothing Then
        For Each pf In pageFields
            Set pfi = New PivotFieldrInfo
            MergePivotFieldsList.Add pfi.SetPivotField(pf, PageType)
        Next pf
    End If
    
    If Not columnFields Is Nothing Then
        For Each pf In columnFields
            If IsNumeric(pf.GroupLevel) Then
                Set pfi = New PivotFieldrInfo
                MergePivotFieldsList.Add pfi.SetPivotField(pf, ColumnType)
            End If
        Next pf
    End If
    
    If Not rowFields Is Nothing Then
        For Each pf In rowFields
            If IsNumeric(pf.GroupLevel) Then
                Set pfi = New PivotFieldrInfo
                MergePivotFieldsList.Add pfi.SetPivotField(pf, RowType)
            End If
        Next pf
    End If
    
End Function



