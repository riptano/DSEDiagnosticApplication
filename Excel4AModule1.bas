Attribute VB_Name = "Module1"
Option Explicit

Public IgnoreWarning As Boolean
Public InitialRefresh As Boolean
Public PTLastRefreshDate As Date
Public PTCacheEnabled As Boolean
Public PTRefreshed As Boolean
Public Initialized As Boolean
Public wshEvents As New Collection
Public Const EnableMacro As String = "**Macros Need to be Enabled and Refresh Executed**"
Const LoadFailure As String = "** Load Failure **"
Const ExceptionDetected As String = "** Exception(s) Detected **"
Const FilterStrDelimator As String = ",. "

Sub LogAggregation_FindLogFileFromLocalTS()
    FindOpenLogFileByLocalTS CStr(Cells(ActiveCell.row, 5).value), CDate(Cells(ActiveCell.row, 14).value)
End Sub

Sub LogInformation_OpenLogFile()
    Dim files As String
    Dim cell As Object
    
    For Each cell In Selection
        files = files & """" & CStr(cell) & """ "
    Next cell
        
    OpenLogFile files
End Sub

Function OpenLogFile(files As String)

    ChDir Application.ThisWorkbook.Path
    
    'MsgBox files & " item(s) selected"
     Shell "notepad " & files, vbNormalFocus
    
End Function

Function FindOpenLogFileByLocalTS(nodeIPAddress As String, localTS As Date)

    Dim rngTargetTSA As Range
    Dim rngTargetTSB As Range
    Dim rngTargetNode As Range
    Dim lngRowCounter As Long
    Dim ws As Worksheet
    Dim foundPaths As String
    
    
    Set ws = Worksheets("LogInformation")
    lngRowCounter = 2
    Set rngTargetTSA = ws.Range("G" & lngRowCounter)
    Set rngTargetTSB = ws.Range("H" & lngRowCounter)
    Set rngTargetNode = ws.Range("A" & lngRowCounter)
    
    Do While Not IsEmpty(rngTargetNode.value)
        If CStr(rngTargetNode.value) = nodeIPAddress And localTS >= CDate(rngTargetTSA.value) And localTS <= CDate(rngTargetTSB.value) Then
            foundPaths = foundPaths & """" & CStr(ws.Range("M" & lngRowCounter).value) & """ "
        End If
    
        lngRowCounter = lngRowCounter + 1
        Set rngTargetTSA = ws.Range("G" & lngRowCounter)
        Set rngTargetTSB = ws.Range("H" & lngRowCounter)
        Set rngTargetNode = ws.Range("A" & lngRowCounter)
    
    Loop
        
    If foundPaths = "" Then
        MsgBox "Could not find Log File for Local Timestamp '" & localTS & "' for node '" & nodeIPAddress & "'.", vbOKOnly, "Search for Log File Failed"
        Exit Function
    End If
        
    'CopyText Format(localTS, "yyyy-mm-dd hh:nn:ss,")

    'MsgBox foundPaths & " path(s) found"
    OpenLogFile foundPaths
        
End Function

Sub CopyText(Text As String)
    Dim MSForms_DataObject As Object
    Set MSForms_DataObject = CreateObject("new:{1C3B4210-F441-11CE-B9EA-00AA006B1A69}")
    MSForms_DataObject.SetText Text
    MSForms_DataObject.PutInClipboard
    Set MSForms_DataObject = Nothing
End Sub

Sub ReFreshPivotTables()
 
    'On Error GoTo Done
    
    If PTCacheEnabled Or PTRefreshed Then
        If MsgBox("A Refresh is not Reqired. Do you really wish to Refresh?", vbQuestion + vbYesNo, "Do you really wish to Refresh?") = vbNo Then GoTo Done
    End If
          
    Dim ProgressBar As New ProgressBar
    Dim totalUpdates As Integer
    Dim ws As Worksheet
    Dim PT As PivotTable
    Dim pc As PivotCache
    Dim ptRefreshStart As Date
    Dim levelGroup As PivotField
    Dim dcipGroup As PivotField
    Dim pf As PivotField
    Dim totSegments As Integer
    Dim curSegment As Integer
        
    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Refreshing..."
    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 37
    ThisWorkbook.Worksheets("Refresh").Cells(9, 1).value = "Ctrl+Break to Suspend"
    ThisWorkbook.Worksheets("Refresh").Cells(9, 1).Interior.ColorIndex = 19
    
    Application.Wait (Now + TimeValue("0:00:1"))
    
    totalUpdates = 4
    
    Application.EnableEvents = False
    Application.DisplayAlerts = False
    Application.ScreenUpdating = False
    
    
    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Refresh Failed..."
    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 3
    
    Call ProgressBar.update(1, totalUpdates, "Refreshing Pivot Tables...", False)
    ptRefreshStart = Now
    
    Initialized = ThisWorkbook.Worksheets("Refresh").Cells(7, 3).value
    
    ThisWorkbook.RefreshAll
            
    'On Error Resume Next
    totSegments = ThisWorkbook.PivotCaches.Count
    curSegment = 1
        
    For Each pc In ThisWorkbook.PivotCaches
            
        If pc.RefreshDate < PTLastRefreshDate Or Not PTRefreshed Then
            Call ProgressBar.update(curSegment, totSegments, "Refreshing Cache " & pc.RefreshName & "...", True)
                    
            pc.Refresh
        Else
            Call ProgressBar.update(curSegment, totSegments, "Skip Refresh Cache " & pc.RefreshName & "...", True)
        End If
        curSegment = curSegment + 1
    Next pc
    'On Error GoTo Done
    
    Call ProgressBar.update(2, totalUpdates, "Please Wait...", False)
    
    PTLastRefreshDate = ptRefreshStart
            
    If Not Initialized Then
        
        Dim pivotFilterWSName As String: pivotFilterWSName = ThisWorkbook.Worksheets("Refresh").Cells(40, 4).value
        Dim pivotExpandWSName As String: pivotExpandWSName = ThisWorkbook.Worksheets("Refresh").Cells(42, 4).value
    
        
        totSegments = ThisWorkbook.Worksheets.Count * 2
        curSegment = 1
        
        For Each ws In ThisWorkbook.Worksheets
            Call ProgressBar.update(curSegment, totSegments, "Upd Flits " & ws.Name & "...", True)
            CheckPivotFilterValues ThisWorkbook.Worksheets(pivotFilterWSName), ws
            curSegment = curSegment + 1
            
            Call ProgressBar.update(curSegment, totSegments, "Row Adjust " & ws.Name & "...", True)
            ExpandPivotFields ThisWorkbook.Worksheets(pivotExpandWSName), ws
            curSegment = curSegment + 1
        Next ws
    
        Initialized = True
        ThisWorkbook.Worksheets("Refresh").Cells(7, 3).value = Initialized
    End If
    
    PTRefreshed = True
    ThisWorkbook.Worksheets("Refresh").Cells(1, 7).value = DateTime.Now
        
    Call ProgressBar.update(3, totalUpdates, "Almost Done...", False)
    
Done:

    UpdateRefreshWSState
        
    Call ProgressBar.update(4, totalUpdates, "Done...", False)
    ThisWorkbook.Worksheets("Refresh").Cells(9, 1).value = Empty
    ThisWorkbook.Worksheets("Refresh").Cells(9, 1).Interior.ColorIndex = 0
    
    Application.ScreenUpdating = True
    Application.DisplayAlerts = True
    Application.EnableEvents = True
    
End Sub

Function CollectionContains(myCol As Collection, checkVal As Variant) As Boolean
    On Error Resume Next
    CollectionContains = False
    Dim it As Variant
    For Each it In myCol
        If it = checkVal Then
            CollectionContains = True
            Exit Function
        End If
    Next
    On Error GoTo 0
End Function
Sub UpdateCacheEnabled()
    Dim ws As Worksheet
    Dim PT As PivotTable
    
    PTCacheEnabled = False
    
    For Each ws In ThisWorkbook.Worksheets
        For Each PT In ws.PivotTables
            On Error Resume Next
            
            If PT.SaveData Then
                PTCacheEnabled = True
            Else
                PTCacheEnabled = False
                Exit For
            End If
        Next PT
    Next ws
End Sub
Sub UpdateRefreshWSState()
    
    UpdateCacheEnabled
                       
    ThisWorkbook.Worksheets("Refresh").Cells(6, 3).value = PTCacheEnabled
               
    If IsEmpty(ThisWorkbook.Worksheets("Application").Cells(2, 1)) Or IgnoreWarning Then
        If PTCacheEnabled Then
            If PTRefreshed Then
                ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Refreshed/Cached"
            Else
                ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Cached"
            End If
            InitialRefresh = False
            ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 43
        Else
            If PTRefreshed Then
                If InitialRefresh Then
                    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Refreshed"
                Else
                    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Cached Disabled"
                    InitialRefresh = True
                End If
                ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 43
            Else
                If ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 43 Or ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 20 Then
                    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Note: Need to Refresh for any Edits"
                    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 20
                Else
                    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Refresh Required!"
                    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 3
                End If
            End If
        End If
    ElseIf ThisWorkbook.Worksheets("Application").Cells(2, 1) = LoadFailure Then
        ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Error: Excel did not properly load this Workbook. Data is probably not Valid"
        ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 3
    ElseIf ThisWorkbook.Worksheets("Application").Cells(2, 1) = ExceptionDetected Then
        ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Warning: Exceptions detected during Workbook Generation, Review 'Application' worksheet for Details."
        ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 6
        ThisWorkbook.Worksheets("Application").Tab.ColorIndex = 3
        If Not PTCacheEnabled And Not PTRefreshed Then
            ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value & " Refresh Required!"
        End If
        IgnoreWarning = True
    Else
        ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = ThisWorkbook.Worksheets("Application").Cells(2, 1)
        ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 45
    End If
    
    ThisWorkbook.Worksheets("Refresh").Cells(1, 9).value = ThisWorkbook.Worksheets("Application").Cells(8, 1) & Chr(10) & ThisWorkbook.Worksheets("Application").Cells(11, 1)
    ThisWorkbook.Worksheets("Refresh").Cells(1, 10).value = ThisWorkbook.Worksheets("Application").Cells(1, 1) & Chr(10) & ThisWorkbook.Worksheets("Application").Cells(4, 1)
    
    If Application.Version < 16 Then
        ThisWorkbook.Worksheets("Refresh").Cells(18, 6).value = "Warning: This workbook requires MS-Excel Version 16 or Higher!"
        ThisWorkbook.Worksheets("Refresh").Cells(18, 6).Interior.ColorIndex = 6
    Else
        ThisWorkbook.Worksheets("Refresh").Cells(18, 1).Interior.ColorIndex = 43
    End If
        
    #If Win64 Then
        ThisWorkbook.Worksheets("Refresh").Cells(18, 1).value = "MS-Excel 64-BIT Version " & Application.Version
    #Else
        ThisWorkbook.Worksheets("Refresh").Cells(18, 1).value = "Excel 32-BIT Version " & Application.Version
        ThisWorkbook.Worksheets("Refresh").Cells(18, 1).Interior.ColorIndex = 6
        ThisWorkbook.Worksheets("Refresh").Cells(19, 6).value = "64-Bit MS-Excel Version Recommended for proper operation!"
        ThisWorkbook.Worksheets("Refresh").Cells(19, 6).Interior.ColorIndex = 6
    #End If
    
    Dim checkedLogAggregation As Boolean
    Dim checkedAggregatedStats As Boolean
    Dim ws As Worksheet
    
    For Each ws In ThisWorkbook.Worksheets
    
        If Not checkedLogAggregation And InStr(1, ws.Name, "LogAggregation-") = 1 Then
            checkedLogAggregation = True
            ThisWorkbook.Worksheets("Refresh").Cells(20, 6).Interior.ColorIndex = 6
            ThisWorkbook.Worksheets("Refresh").Cells(20, 6).value = "Warning: Table 'LogAggregation' spans multiple worksheets!"
        ElseIf Not checkedAggregatedStats And InStr(1, ws.Name, "AggregatedStats-") = 1 Then
            checkedAggregatedStats = True
            ThisWorkbook.Worksheets("Refresh").Cells(21, 6).Interior.ColorIndex = 6
            ThisWorkbook.Worksheets("Refresh").Cells(21, 6).value = "Warning: Table 'AggregatedStats' spans multiple worksheets!"
        End If
                
    Next ws


End Sub

Sub UpdatePivotCache(ByVal cacheEnabled As Boolean)
    On Error GoTo Done
    
    If cacheEnabled = PTCacheEnabled Then GoTo Done
        
    Dim ProgressBar As New ProgressBar
    Dim totalUpdates As Integer
    Dim updateNum As Integer
    Dim ws As Worksheet
    Dim PT As PivotTable
    Dim refreshed As Boolean
        
    refreshed = False
    
    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Updating Cache..."
    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 37
    Application.Wait (Now + TimeValue("0:00:1"))
    
    totalUpdates = 1
    
    Application.EnableEvents = False
    Application.DisplayAlerts = False
    Application.ScreenUpdating = False
    
    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Cache Update Failed..."
    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 3
      
    updateNum = 1
    For Each ws In ThisWorkbook.Worksheets
        For Each PT In ws.PivotTables
            On Error Resume Next
            
            If PT.SaveData = cacheEnabled Then
                Call ProgressBar.update(updateNum, totalUpdates, "Cache Already Set for " & PT.Name & "...", False)
            Else
                
                PT.SaveData = cacheEnabled
                
                If cacheEnabled And Not PT.SaveData Then
                    Call ProgressBar.update(updateNum, totalUpdates, "Refreshing PT " & PT.Name & "...", False)
                    PT.PivotCache.Refresh
                    refreshed = True
                    PT.SaveData = True
                End If
                                                
            End If
        Next PT
    Next ws
       
    If cacheEnabled And refreshed Then
        PTRefreshed = True
        ThisWorkbook.Worksheets("Refresh").Cells(1, 7).value = DateTime.Now
    End If
    
Done:

    UpdateRefreshWSState
      
    Application.ScreenUpdating = True
    Application.DisplayAlerts = True
    Application.EnableEvents = True
   
End Sub

Sub CheckBox2_Click()

    Dim cacheValue As Boolean
    
    cacheValue = ThisWorkbook.Worksheets("Refresh").Cells(6, 3).value
    
    If PTCacheEnabled = cacheValue Then Exit Sub
    
    If cacheValue Then
        If MsgBox("Enabling the Pivot Cache will dramatically increase the size of this WorkBook resulting in longer load and save times! Are you sure in enabling the cache?", vbQuestion + vbYesNo, "Are you sure you wish to enable the cache?") = vbNo Then GoTo Done
    End If
        
    UpdatePivotCache cacheValue
    
Done:
    
    ThisWorkbook.Worksheets("Refresh").Cells(6, 3).value = PTCacheEnabled

End Sub

Function PivotFilterValues(targetWS As Worksheet)
       
    Dim ws As Worksheet
    Dim PT As PivotTable
    Dim row As Integer: row = 1
    Dim pfInfo As PivotFieldrInfo
    Dim Factory As Factory: Set Factory = New Factory
    
    targetWS.UsedRange.Clear
    
    targetWS.Cells(row, 1).value = "Worksheet"
    targetWS.Cells(row, 2).value = "PivotTable"
    targetWS.Cells(row, 3).value = "Field"
    targetWS.Cells(row, 4).value = "Filter Values"
    targetWS.Cells(row, 5).value = "PT Area"
    targetWS.Cells(row, 6).value = "Always Filter Value"
    targetWS.Cells(row, 7).value = "Label Type"
    targetWS.Cells(row, 8).value = "Label Value1"
    targetWS.Cells(row, 9).value = "Label Value2"
    targetWS.Cells(row, 10).value = "Value Type"
    targetWS.Cells(row, 11).value = "Value Value1"
    targetWS.Cells(row, 12).value = "Value Value2"
    targetWS.Cells(row, 13).value = DateTime.Now
    
    row = row + 1
    
    For Each ws In ThisWorkbook.Worksheets
        For Each PT In ws.PivotTables
            For Each pfInfo In Factory.MergePivotFieldsList(PT.pageFields, PT.columnFields, PT.rowFields)
                
                    targetWS.Cells(row, 1).value = ws.Name
                    targetWS.Cells(row, 2).value = PT.Name
                    targetWS.Cells(row, 3).value = pfInfo.Name
                    targetWS.Cells(row, 5).value = pfInfo.PivotTypeString
                    targetWS.Cells(row, 4).value = pfInfo.value
                    
                    If Not pfInfo.LabelFilter Is Nothing Then
                        targetWS.Cells(row, 7).value = TranslatePivotTypeFilterVar(pfInfo.LabelFilter.FilterType)
                        targetWS.Cells(row, 8).value = pfInfo.LabelFilter.Value1
                        targetWS.Cells(row, 9).value = pfInfo.LabelFilter.Value2
                    End If
                     If Not pfInfo.ValueFilter Is Nothing Then
                        targetWS.Cells(row, 10).value = TranslatePivotTypeFilterVar(pfInfo.ValueFilter.FilterType)
                        targetWS.Cells(row, 11).value = pfInfo.ValueFilter.Value1
                        targetWS.Cells(row, 12).value = pfInfo.ValueFilter.Value2
                    End If
                   
                    row = row + 1
            Next pfInfo
        Next PT
    Next ws

End Function
Function PivotRowCollapseExpandValues(targetWS As Worksheet)
       
    Dim ws As Worksheet
    Dim PT As PivotTable
    Dim row As Integer: row = 1
    Dim pfInfo As PivotFieldrInfo
    Dim Factory As Factory: Set Factory = New Factory
    
    targetWS.UsedRange.Clear
    
    targetWS.Cells(row, 1).value = "Worksheet"
    targetWS.Cells(row, 2).value = "PivotTable"
    targetWS.Cells(row, 3).value = "Field"
    targetWS.Cells(row, 4).value = "Expanded"
    targetWS.Cells(row, 5).value = DateTime.Now
    
    row = row + 1
        
     For Each ws In ThisWorkbook.Worksheets
        For Each PT In ws.PivotTables
            PT.ManualUpdate = True
            
            For Each pfInfo In Factory.MergePivotFieldsList(Nothing, Nothing, PT.rowFields)
                If pfInfo.CanExpand Then
                    targetWS.Cells(row, 1).value = ws.Name
                    targetWS.Cells(row, 2).value = PT.Name
                    targetWS.Cells(row, 3).value = pfInfo.Name
                    targetWS.Cells(row, 4).value = pfInfo.IsExpanded
                    row = row + 1
                End If
            Next pfInfo
            
            PT.ManualUpdate = False
        Next PT
    Next ws

End Function

Function CheckPivotFilterValues(pivotFilterValuesWS As Worksheet, targetWS As Worksheet)

    Dim wsFilterDataArray() As Variant: wsFilterDataArray = pivotFilterValuesWS.UsedRange.value
          
    Dim PT As PivotTable
    Dim pfInfo As PivotFieldrInfo
    Dim filterData As WSFilterData
    Dim pivotFilters As Collection
    Dim pivotFields As pivotFields
    Dim Factory As Factory: Set Factory = New Factory
    Dim fndPFInfos As Collection
    Dim pvtFldType As PivotTypes
    
    For Each PT In targetWS.PivotTables
        PT.ManualUpdate = True
        
        Set pivotFilters = Factory.GetPivotFilterInfoForPivotTbl(wsFilterDataArray, targetWS.Name, PT.Name)
        
        For Each filterData In pivotFilters
        
            If filterData.Active = True Then
                Select Case filterData.pvtFldType
                   Case "Page"
                        Set pivotFields = PT.pageFields
                        pvtFldType = PageType
                   Case "Column"
                        Set pivotFields = PT.columnFields
                        pvtFldType = ColumnType
                   Case "Row"
                        Set pivotFields = PT.rowFields
                        pvtFldType = RowType
                   Case Else
                        Set pivotFields = Nothing
                        pvtFldType = Unknown
                End Select
                
                Set pfInfo = Factory.FindPivotFieldInfo(pivotFields, filterData.FldName, pvtFldType)
                    
                If pfInfo Is Nothing Then
                    Debug.Print "WSFilterFld NotFound", "'" + targetWS.Name + "'", "'" + PT.Name + "'", "'" + filterData.pvtFldType + "'", "'" + filterData.FldName + "'"
                Else
                    Set fndPFInfos = pfInfo.MatchAndSelectString(IIf(filterData.SetFilterValue = Empty, filterData.StaticFilterValue, filterData.SetFilterValue), Not filterData.SetFilterValue = Empty, True)
                
                    If fndPFInfos.Count = 0 Then
                        Debug.Print "NoMatch", "'" + targetWS.Name + "'", "'" + PT.Name + "'", "'" + pfInfo.Name + "'", "Static: '" & filterData.StaticFilterValue & "'", "Set: '" & filterData.SetFilterValue & "'"
                    End If
                End If
            End If
        Next filterData
        
        PT.ManualUpdate = False
    Next PT
   
End Function

Function ExpandPivotFields(pivotExpandFldsWS As Worksheet, targetWS As Worksheet)

    Dim wsRowExpandArray() As Variant: wsRowExpandArray = pivotExpandFldsWS.UsedRange.value
    
    Dim PT As PivotTable
    Dim pfInfo As PivotFieldrInfo
    Dim rowData As WSRowExpandData
    Dim pivotRows As Collection
    Dim pivotFields As pivotFields
    Dim Factory As Factory: Set Factory = New Factory
    Dim fndPFInfos As Collection
    
    For Each PT In targetWS.PivotTables
        PT.ManualUpdate = True
        
        Set pivotRows = Factory.GetRowExpandInforPivotTbl(wsRowExpandArray, targetWS.Name, PT.Name)
        
        For Each rowData In pivotRows
        
            If rowData.Active = True Then
                Set pfInfo = Factory.FindPivotFieldInfo(PT.rowFields, rowData.FldName, RowType)
                
                If pfInfo Is Nothing Then
                    Debug.Print "WSRowFld NotFound", "'" + targetWS.Name + "'", "'" + PT.Name + "'", "'" + rowData.FldName + "'"
                Else
                    Call pfInfo.Expand(rowData.ExpandRow)
                End If
            End If
        Next rowData
        
        PT.ManualUpdate = False
    Next PT
      
End Function

Function CreatWSIfNotExists(WSName As String) As Worksheet
    Dim wsTest As Worksheet: Set wsTest = Nothing
    
    On Error Resume Next
    Set wsTest = ThisWorkbook.Worksheets(WSName)
    On Error GoTo 0
     
    If wsTest Is Nothing Then
        ThisWorkbook.Worksheets.Add.Name = WSName
        Set CreatWSIfNotExists = ThisWorkbook.Worksheets(WSName)
    Else
        Set CreatWSIfNotExists = wsTest
    End If
    
End Function

Sub ResetPivotTables()

    Dim pf As PivotField
    Dim ws As Worksheet
    Dim PT As PivotTable
    Dim pi As pivotItem
    Dim pc As PivotCache
    Dim generatePivotFilterWS As Boolean: generatePivotFilterWS = ThisWorkbook.Worksheets("Refresh").Cells(39, 4).value
    Dim generatePivotExpandWS As Boolean: generatePivotExpandWS = ThisWorkbook.Worksheets("Refresh").Cells(41, 4).value
    Dim pivotFilterWSName As String: pivotFilterWSName = ThisWorkbook.Worksheets("Refresh").Cells(40, 4).value
    Dim pivotExpandWSName As String: pivotExpandWSName = ThisWorkbook.Worksheets("Refresh").Cells(42, 4).value
    
    If MsgBox("Do you Really wish to re-initilized this workbook?", vbQuestion + vbYesNo, "Re-initilize Workbook") = vbNo Then Exit Sub
        
    For Each pc In ThisWorkbook.PivotCaches
        pc.MissingItemsLimit = xlMissingItemsNone
    Next pc
    
    For Each ws In ThisWorkbook.Worksheets
        For Each PT In ws.PivotTables
            'On Error Resume Next
            PT.PivotCache.MissingItemsLimit = xlMissingItemsNone
        Next PT
    Next ws
    
    PTCacheEnabled = False
    PTRefreshed = False
    
    UpdatePivotCache False
    Initialized = True
    
    ThisWorkbook.Worksheets("Refresh").Cells(7, 3).value = Initialized
    ThisWorkbook.Worksheets("Refresh").Cells(6, 3).value = PTCacheEnabled
    
    Call ReFreshPivotTables
    
    For Each ws In ThisWorkbook.Worksheets
        For Each PT In ws.PivotTables
            'On Error Resume Next
            For Each pf In PT.pageFields
                pf.IncludeNewItemsInFilter = True
            Next pf
        Next PT
    Next ws
    
    ThisWorkbook.ShowPivotTableFieldList = False
        
    
    If generatePivotFilterWS = True Then
        Call PivotFilterValues(CreatWSIfNotExists(pivotFilterWSName))
    End If
    If generatePivotExpandWS = True Then
        Call PivotRowCollapseExpandValues(CreatWSIfNotExists(pivotExpandWSName))
    End If
        
    For Each ws In ThisWorkbook.Worksheets
        ws.Cells(1, 1).Show
    Next ws
    
    PTCacheEnabled = False
    PTRefreshed = False
    Initialized = False
    PTRefreshed = False
    
    ThisWorkbook.Worksheets("Refresh").Cells(7, 3).value = Initialized
    ThisWorkbook.Worksheets("Refresh").Cells(6, 3).value = PTCacheEnabled
    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).value = "Refesh Required!"
    ThisWorkbook.Worksheets("Refresh").Cells(1, 1).Interior.ColorIndex = 3
    ThisWorkbook.Worksheets("Refresh").Cells(18, 1).value = "Warning: Cannot Determine Excel Type or Version"
    ThisWorkbook.Worksheets("Refresh").Cells(18, 1).Interior.ColorIndex = 3
    ThisWorkbook.Worksheets("Refresh").Cells(1, 7).value = Empty
    ThisWorkbook.Worksheets("Refresh").Cells(17, 6).value = Empty
    ThisWorkbook.Worksheets("Refresh").Cells(17, 6).Interior.ColorIndex = 0
    ThisWorkbook.Worksheets("Refresh").Cells(18, 6).value = Empty
    ThisWorkbook.Worksheets("Refresh").Cells(18, 6).Interior.ColorIndex = 0
    ThisWorkbook.Worksheets("Refresh").Cells(18, 6).value = Empty
    ThisWorkbook.Worksheets("Refresh").Cells(18, 6).Interior.ColorIndex = 0
    ThisWorkbook.Worksheets("Refresh").Cells(20, 6).value = Empty
    ThisWorkbook.Worksheets("Refresh").Cells(20, 6).Interior.ColorIndex = 0
    ThisWorkbook.Worksheets("Refresh").Cells(21, 6).value = Empty
    ThisWorkbook.Worksheets("Refresh").Cells(21, 6).Interior.ColorIndex = 0
    ThisWorkbook.Worksheets("Refresh").Cells(1, 9).value = Empty
    ThisWorkbook.Worksheets("Refresh").Cells(1, 10).value = Empty
    ThisWorkbook.Worksheets("Refresh").Cells(2, 1).Show
    ThisWorkbook.Worksheets("Refresh").Activate
    
Done:

End Sub

Sub test()
    Dim ws As Worksheet: Set ws = ThisWorkbook.Worksheets("Read Table")
    Dim pfInfo As PivotFieldrInfo
    'Dim ws As Worksheet
    Dim PT As PivotTable
    Dim pfItem As PivotItemInfo
    Dim Factory As Factory: Set Factory = New Factory
    Dim ptField As PivotField
    On Error GoTo 0
    
    Debug.Print "===="
    
    
    'Set ptField = ThisWorkbook.Worksheets("Storage Table").PivotTables("PivotTableStorageTable").rowFields("Node IPAddress")  '.columnFields("Attribute") '.CurrentPage
    
    'Call PivotFilterValues(CreatWSIfNotExists("Tst1"))
    'Call CheckPivotFilterValues(CreatWSIfNotExists("PivotFilterValues"), CreatWSIfNotExists("Parsing Errors"))
    'Call ExpandPivotFields(CreatWSIfNotExists("PivotExpandValues"), CreatWSIfNotExists("Read Table"))
    
    For Each PT In ws.PivotTables
        PT.ManualUpdate = True
        
        For Each pfInfo In Factory.MergePivotFieldsList(Nothing, PT.columnFields, Nothing)
        'For Each pfInfo In Factory.MergePivotFieldsList(PT.pageFields, Nothing, Nothing)
            Debug.Print "'" + ws.Name + "'", "'" + PT.Name + "'", "'" + pfInfo.Name + "'"

'            If pfInfo.Name = "CQL Type" Then
'                Call pfInfo.MatchAndSelectString("(All)", False, True)
'            Else
'                Call pfInfo.SelectAll
'            End If
            'Call pfInfo.MatchAndSelectString("Local read latency,. Local read count", False, True)
                       
            For Each pfItem In pfInfo.MatchAndSelectString("Local read latency,. Local read count", False, True) 'pfInfo.Selected
                Debug.Print "New Selected", pfItem.Name, pfItem.Selected
            Next pfItem
            'pfInfo.Field.ClearManualFilter
             'Debug.Print "'" + ws.Name + "'", "'" + PT.Name + "'", "'" + pfInfo.Name + "'", pfInfo.Selected.Count, pfInfo.Selected(1).Name, pfInfo.Selected(1).Selected
            Debug.Print "'" + ws.Name + "'", "'" + PT.Name + "'", "'" + pfInfo.Name + "'", pfInfo.MultiSelection, pfInfo.Selected.Count
            For Each pfItem In pfInfo.Items
                Debug.Print "  All-Items", pfItem.Name, pfItem.Selected
            Next pfItem
            For Each pfItem In pfInfo.Selected
                Debug.Print "  Selected", pfItem.Name, pfItem.Selected
            Next pfItem
             'If pf.Name = "Attribute" Then
                '  Call EnablePivotFieldBasedOnPattern(pf, "Space used*,. SSTable count,. Number of keys*,. Number of partitions*,. *storage*", element.PT = 1)
            'End If
        Next pfInfo
        
        PT.ManualUpdate = False
    Next PT
    
End Sub

