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
    Dim pf As PivotField
    Dim pfi As PivotFieldrInfo
    On Error GoTo 0
    
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

Public Function FindPivotFieldInfo(pivotFields As pivotFields, fieldName As String, pivotType As PivotTypes) As PivotFieldrInfo
    On Error Resume Next
        
    Set FindPivotFieldInfo = Nothing
    
    If IsEmpty(fieldName) = True Or fieldName = Empty Then GoTo Done
    
    Dim pf As PivotField: Set pf = pivotFields(fieldName)
    
    If pf Is Nothing Then
        If pivotType = PageType Then
            For Each pf In pivotFields
                If pf.SourceName = fieldName Then
                    Exit For
                End If
            Next pf
        Else
            For Each pf In pivotFields
                If IsNumeric(pf.GroupLevel) And pf.SourceName = fieldName Then
                    Exit For
                End If
            Next pf
        End If
    End If
    
    Set FindPivotFieldInfo = New PivotFieldrInfo
            
    Call FindPivotFieldInfo.SetPivotField(pf, pivotType)
    
Done:

    On Error GoTo 0
    
End Function

Function GetFilterInfoForField(wsFilterArray() As Variant, WSName As String, pivotTblName As String, fieldName As String) As WSFilterData
    Dim foundPivotTbl As Boolean: foundPivotTbl = False
    Dim i As Integer
    'On Error Resume Next
        
    Set GetFilterInfoForField = Nothing

    For i = LBound(wsFilterArray, 1) To UBound(wsFilterArray, 1)
        If wsFilterArray(i, 1) = WSName Then
            If wsFilterArray(i, 2) = pivotTblName Then
                foundPivotTbl = True
                If wsFilterArray(i, 3) = fieldName Then
                    Set GetFilterInfoForField = New WSFilterData
                    GetFilterInfoForField.Active = True
                    GetFilterInfoForField.WSName = wsFilterArray(i, 1)
                    GetFilterInfoForField.PTName = wsFilterArray(i, 2)
                    GetFilterInfoForField.FldName = wsFilterArray(i, 3)
                    GetFilterInfoForField.pvtFldType = wsFilterArray(i, 5)
                    GetFilterInfoForField.StaticFilterValue = wsFilterArray(i, 4)
                    GetFilterInfoForField.SetFilterValue = wsFilterArray(i, 6)
                    GetFilterInfoForField.LblType = wsFilterArray(i, 7)
                    GetFilterInfoForField.LblValue1 = wsFilterArray(i, 8)
                    GetFilterInfoForField.LblValue2 = wsFilterArray(i, 9)
                    GetFilterInfoForField.ValType = wsFilterArray(i, 10)
                    GetFilterInfoForField.ValValue1 = wsFilterArray(i, 11)
                    GetFilterInfoForField.ValValue2 = wsFilterArray(i, 12)
                    Exit For
                End If
            Else
                If foundPivotTbl Then
                    Exit For
                End If
            End If
        Else
            If foundPivotTbl Then
                Exit For
            End If
        End If
    Next i
End Function

Function GetPivotFilterInfoForPivotTbl(wsFilterArray() As Variant, WSName As String, pivotTblName As String) As Collection

    Dim foundPivotTbl As Boolean: foundPivotTbl = False
    Dim i As Integer
    Dim wsInfo As WSFilterData
                 
    'On Error Resume Next
        
    Set GetPivotFilterInfoForPivotTbl = New Collection
    
    For i = LBound(wsFilterArray, 1) To UBound(wsFilterArray, 1)
        If wsFilterArray(i, 1) = WSName Then
            If wsFilterArray(i, 2) = pivotTblName Then
                foundPivotTbl = True
                Set wsInfo = New WSFilterData
                
                wsInfo.Active = True
                wsInfo.WSName = wsFilterArray(i, 1)
                wsInfo.PTName = wsFilterArray(i, 2)
                wsInfo.FldName = wsFilterArray(i, 3)
                wsInfo.pvtFldType = wsFilterArray(i, 5)
                wsInfo.StaticFilterValue = wsFilterArray(i, 4)
                wsInfo.SetFilterValue = wsFilterArray(i, 6)
                wsInfo.LblType = wsFilterArray(i, 7)
                wsInfo.LblValue1 = wsFilterArray(i, 8)
                wsInfo.LblValue2 = wsFilterArray(i, 9)
                wsInfo.ValType = wsFilterArray(i, 10)
                wsInfo.ValValue1 = wsFilterArray(i, 11)
                wsInfo.ValValue2 = wsFilterArray(i, 12)
                GetPivotFilterInfoForPivotTbl.Add wsInfo
            Else
                If foundPivotTbl Then
                    Exit For
                End If
            End If
        Else
            If foundPivotTbl Then
                Exit For
            End If
        End If
    Next i
End Function

Function GetRowExpandInfoForField(wsExpandArray() As Variant, WSName As String, pivotTblName As String, fieldName As String) As WSRowExpandData

    Dim foundPivotTbl As Boolean: foundPivotTbl = False
    Dim i As Integer
       
    Set GetRowExpandInfoForField = Nothing
    
    For i = LBound(wsExpandArray, 1) To UBound(wsExpandArray, 1)
        If wsExpandArray(i, 1) = WSName Then
            If wsExpandArray(i, 2) = pivotTblName Then
                foundPivotTbl = True
                If wsExpandArray(i, 3) = fieldName Then
                    Set GetRowExpandInfoForField = New WSRowExpandData
                    GetRowExpandInfoForField.Active = True
                    GetRowExpandInfoForField.WSName = wsExpandArray(i, 1)
                    GetRowExpandInfoForField.PTName = wsExpandArray(i, 2)
                    GetRowExpandInfoForField.FldName = wsExpandArray(i, 3)
                    GetRowExpandInfoForField.ExpandRow = wsExpandArray(i, 4)
                    Exit For
                End If
            Else
                If foundPivotTbl Then
                    Exit For
                End If
            End If
        Else
            If foundPivotTbl Then
                Exit For
            End If
        End If
    Next i
End Function

Function GetRowExpandInforPivotTbl(wsExpandArray() As Variant, WSName As String, pivotTblName As String) As Collection

    Dim foundPivotTbl As Boolean: foundPivotTbl = False
    Dim rowInfo As WSRowExpandData
    Dim i As Integer
       
    Set GetRowExpandInforPivotTbl = New Collection
    
    For i = LBound(wsExpandArray, 1) To UBound(wsExpandArray, 1)
        If wsExpandArray(i, 1) = WSName Then
            If wsExpandArray(i, 2) = pivotTblName Then
                foundPivotTbl = True
                Set rowInfo = New WSRowExpandData
                rowInfo.Active = True
                rowInfo.WSName = wsExpandArray(i, 1)
                rowInfo.PTName = wsExpandArray(i, 2)
                rowInfo.FldName = wsExpandArray(i, 3)
                rowInfo.ExpandRow = wsExpandArray(i, 4)
                GetRowExpandInforPivotTbl.Add rowInfo
            Else
                If foundPivotTbl Then
                    Exit For
                End If
            End If
        Else
            If foundPivotTbl Then
                Exit For
            End If
        End If
    Next i
End Function

