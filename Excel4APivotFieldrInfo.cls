VERSION 1.0 CLASS
BEGIN
  MultiUse = -1  'True
END
Attribute VB_Name = "PivotFieldrInfo"
Attribute VB_GlobalNameSpace = False
Attribute VB_Creatable = False
Attribute VB_PredeclaredId = False
Attribute VB_Exposed = False
'VERSION 1.0 CLASS

Option Explicit
'Unrestricted class

Public Enum PivotTypes
 Unknown = 0
 PageType = 1
 ColumnType = 2
 RowType = 3
End Enum

Public Field As pivotField
Public Name As String
Public Items As Collection
Public PivotType As PivotTypes
Public LabelFilter As PivotFilter
Public ValueFilter As PivotFilter
Public BlankItem As pivotItemInfo
Public AllItem As pivotItemInfo

Const FilterStrDelimator As String = ",. "


Private Sub Class_Initialize()
    
    Me.PivotType = Unknown
    Me.Name = Empty
    
    Set Me.LabelFilter = Nothing
    Set Me.ValueFilter = Nothing
    Set Me.BlankItem = Nothing
    Set Me.AllItem = Nothing
    
End Sub

Public Sub SetName(pField As pivotField)
    On Error Resume Next
    
    Dim sSetName As String
    
    sSetName = Trim(pField.SourceName)
    
    If IsEmpty(sSetName) = True Then
        sSetName = Trim(pField.Name)
    End If
         
    Me.Name = sSetName
    
    On Error GoTo 0
    
End Sub

Public Function SetPivotField(pField As pivotField, pType As PivotTypes) As PivotFieldrInfo

    Set SetPivotField = Me
    
    Set Me.Field = pField
    Me.PivotType = pType
    Call Me.SetName(pField)
        
    If Not pField.PivotFilters Is Nothing And pField.PivotFilters.Count = 1 Then
        If pField.PivotFilters(1).Active = True Then
            If pField.PivotFilters(1).Order = -1 Then
                Set Me.ValueFilter = pField.PivotFilters(1)
            Else
                Set Me.LabelFilter = pField.PivotFilters(1)
            End If
        End If
    End If
    
    Dim pItem As pivotItem
    Dim pItemInfo As pivotItemInfo
    
    Set Me.Items = New Collection
    
    For Each pItem In pField.PivotItems
        
        Set pItemInfo = New pivotItemInfo
        Call pItemInfo.SetPivotItem(pItem, Me)
        Me.Items.Add pItemInfo
        
        If pItemInfo.IsAll Then
            Set Me.AllItem = pItemInfo
        ElseIf pItemInfo.IsBlank Then
            Set Me.BlankItem = pItemInfo
        End If
        
    Next pItem

End Function

Public Property Get MultiSelection() As Boolean
    If Me.PivotType = PageType Then
        MultiSelection = Me.Field.EnableMultiplePageItems = True
    Else
        MultiSelection = True
    End If
End Property

Public Property Get Selected() As Collection
    Dim pItem As pivotItemInfo
    
    Set Selected = New Collection
    
    If Me.MultiSelection = True Then
        'Dim blankSelected As Boolean: blankSelected = False
        For Each pItem In Me.Items
            If pItem.Selected = True Then
                Selected.Add pItem
                'If pItem.IsBlank Then blankSelected = True
            End If
        Next pItem
        If Me.Items.Count = Selected.Count Then 'IIf(blankSelected = True, Selected.Count, Selected.Count + 1)
            If Me.AllItem Is Nothing Then
                Set Me.AllItem = New pivotItemInfo
                Call Me.AllItem.SetAllPivotItem(Me)
            End If
            Set Selected = Nothing
            Set Selected = New Collection
            Selected.Add Me.AllItem
        End If
    ElseIf Me.Field.CurrentPage = "(All)" Then
        If Me.AllItem Is Nothing Then
            Set Me.AllItem = New pivotItemInfo
            Call Me.AllItem.SetAllPivotItem(Me)
        End If
        Selected.Add Me.AllItem
    ElseIf Me.Field.CurrentPage = "(blank)" Then
        Selected.Add Me.BlankItem
    Else
        For Each pItem In Me.Items
            If pItem.Item.Caption = Me.Field.CurrentPage Then
                Selected.Add pItem
                Exit For
            End If
        Next pItem
    End If
    
End Property

Public Property Get PivotTypeString() As String
    
    Select Case Me.PivotType
        Case PageType
            PivotTypeString = "Page"
        Case ColumnType
            PivotTypeString = "Column"
        Case RowType
            PivotTypeString = "Row"
        Case Else
            PivotTypeString = Empty
    End Select
    
End Property

Public Property Let PivotTypeString(strType As String)
    
    Select Case strType
        Case "Page"
            Me.PivotType = PageType
        Case "Column"
            Me.PivotType = ColumnType
        Case "Row"
            Me.PivotType = RowType
        Case Else
            Me.PivotType = Unknown
    End Select
    
End Property

Public Property Get IsExpanded() As Boolean
    On Error GoTo Done
    
    IsExpanded = False
    
    If Not Me.PivotType = RowType Then Exit Property
    
    If IsNumeric(Me.Field.Position) Then
        If Me.Field.ShowDetail = True Then
            IsExpanded = True
        End If
    End If
    
Done:

     On Error GoTo 0
    
End Property

Public Property Get CanExpand() As Boolean
    On Error GoTo Done
    
    CanExpand = False
    
    If Not Me.PivotType = RowType Then Exit Property
    
    If IsNumeric(Me.Field.Position) Then
        If Me.Field.ShowDetail = True Then
            CanExpand = True
        Else
            CanExpand = True
        End If
    End If
    
Done:

     On Error GoTo 0
    
End Property

Public Function Expand(expandField As Boolean) As PivotFieldrInfo
    On Error GoTo Done
    
    Set Expand = Me
    
    If Not Me.PivotType = RowType Then Exit Function
    
    If IsNumeric(Me.Field.Position) Then
        'If Not Me.Field.ShowDetail = expandField Then
            Me.Field.ShowDetail = expandField
        'End If
    End If
    
Done:

     On Error GoTo 0
    
End Function

Public Property Get SelectedValues() As Collection
    Dim pItem As pivotItemInfo
    
    Set SelectedValues = New Collection
    
    If Me.MultiSelection = True Then
        'Dim blankSelected As Boolean: blankSelected = False
        For Each pItem In Me.Items
            If pItem.Selected = True Then
                SelectedValues.Add pItem.Name
                'If pItem.IsBlank Then blankSelected = True
            End If
        Next pItem
        If Me.Items.Count = SelectedValues.Count Then 'IIf(blankSelected = True, SelectedValues.Count, SelectedValues.Count + 1)
            If Me.AllItem Is Nothing Then
                Set Me.AllItem = New pivotItemInfo
                Call Me.AllItem.SetAllPivotItem(Me)
            End If
            Set SelectedValues = Nothing
            Set SelectedValues = New Collection
            SelectedValues.Add "(All)"
        End If
    ElseIf Me.Field.CurrentPage = "(All)" Then
        If Me.AllItem Is Nothing Then
            Set Me.AllItem = New pivotItemInfo
            Call Me.AllItem.SetAllPivotItem(Me)
        End If
        SelectedValues.Add "(All)"
    Else
        SelectedValues.Add CStr(Me.Field.CurrentPage)
    End If
    
End Property

Public Property Get value() As String
    Dim firstItem As Boolean: firstItem = True
    Dim strValue As Variant
    
    value = Empty
    
    For Each strValue In Me.SelectedValues
        If firstItem = True Then
            value = strValue
            firstItem = False
        Else
            value = value + FilterStrDelimator + strValue
        End If
    Next strValue
    
End Property
Public Property Let value(filterStr As String)
    
    Call Me.MatchAndSelectString(filterStr, False, True)
    
End Property

Public Function ClearFilter() As PivotFieldrInfo
    Set ClearFilter = Me
    
    Me.Field.ClearManualFilter
    
    If Me.MultiSelection = True Then
        Dim pfItem As pivotItemInfo
        Me.BlankItem.Item.Visible = True
        For Each pfItem In Me.Items
            If pfItem.IsBlank = False Then
                pfItem.Selected = False
            End If
        Next pfItem
    Else
        Me.BlankItem.Selected = True
    End If
    
End Function

Public Function SetOnlyBlankItem() As PivotFieldrInfo

    Set SetOnlyBlankItem = Me
    
    Call Me.ClearFilter
    
End Function

Public Function SetAllItem() As PivotFieldrInfo
    Set SetAllItem = Me
    
    If Me.AllItem Is Nothing Then
        Set Me.AllItem = New pivotItemInfo
        Call Me.AllItem.SetAllPivotItem(Me)
    End If
        
    Me.Field.ClearManualFilter
    
    If Me.MultiSelection = True Then
        Dim pfItem As pivotItemInfo
        For Each pfItem In Me.Items
            pfItem.Selected = True
        Next pfItem
    Else
        Me.Field.CurrentPage = "(All)"
    End If
                   
End Function

Public Function Matched(matchValues() As String, patternMatch As Boolean) As Collection
    Dim value As String
    Dim pItem As pivotItemInfo
    Dim valueLBnd As Long: valueLBnd = LBound(matchValues)
    Dim valueUBnd As Long: valueUBnd = UBound(matchValues)
    Dim valueCnt As Long: valueCnt = valueUBnd - valueLBnd + 1
    Dim valueIdx As Long
        
    Set Matched = New Collection
    
    If valueCnt = 0 Then Exit Function
    If valueCnt = 1 And matchValues(valueLBnd) = "(All)" Then
        If Me.AllItem Is Nothing Then
            Set Me.AllItem = New pivotItemInfo
            Call Me.AllItem.SetAllPivotItem(Me)
        End If
        Matched.Add Me.AllItem
    Else
        For Each pItem In Me.Items
            For valueIdx = valueLBnd To valueUBnd
                If IIf(patternMatch = True, pItem.Name Like matchValues(valueIdx), matchValues(valueIdx) = pItem.Name) Then
                    Matched.Add pItem
                    If patternMatch = False And Matched.Count = valueCnt Then Exit Function
                    Exit For
                End If
            Next valueIdx
        Next pItem
    End If
    
End Function

Public Function MatchAndSelect(matchValues() As String, patternMatch As Boolean, resetFilter As Boolean) As Collection
    Dim value As String
    Dim pItem As pivotItemInfo
    Dim valueLBnd As Long: valueLBnd = LBound(matchValues)
    Dim valueUBnd As Long: valueUBnd = UBound(matchValues)
    Dim valueCnt As Long: valueCnt = valueUBnd - valueLBnd + 1
    Dim valueIdx As Long
    
    Set MatchAndSelect = New Collection
    
    If valueCnt = 0 Then Exit Function
    If resetFilter = True Then Me.Field.ClearManualFilter
    
    If valueCnt = 1 Then
        If matchValues(valueLBnd) = "(All)" Then
            Call Me.SetAllItem
            MatchAndSelect.Add Me.AllItem
            Exit Function
        ElseIf matchValues(valueLBnd) = "(blank)" Then
            Call Me.SetOnlyBlankItem
            MatchAndSelect.Add Me.BlankItem
            Exit Function
        End If
    End If
    
    Dim blankMatched As Boolean
    Dim localMS As Boolean: localMS = Me.MultiSelection
     
    If localMS = True Then
        If Me.BlankItem.Item.Visible = False Then
            Me.BlankItem.Item.Visible = True
        ElseIf resetFilter = False Then
            blankMatched = True
        End If
    ElseIf resetFilter = False Then
        For valueIdx = valueLBnd To valueUBnd
            If IIf(patternMatch = True, Me.Field.CurrentPage Like matchValues(valueIdx), matchValues(valueIdx) = Me.Field.CurrentPage) Then
                MatchAndSelect.Add pItem
                Exit Function
            End If
        Next valueIdx
    End If
    
    Dim Selected As Boolean
    Dim checkValues As Boolean: checkValues = True
   
    For Each pItem In Me.Items
        Selected = False
        If checkValues = True Then
            For valueIdx = valueLBnd To valueUBnd
                If IIf(patternMatch = True, pItem.Name Like matchValues(valueIdx), matchValues(valueIdx) = pItem.Name) Then
                    MatchAndSelect.Add pItem
                    pItem.Selected = True
                    If localMS = False Then Exit Function
                    Selected = True
                    If pItem.Name = "(blank)" Then blankMatched = True
                    checkValues = patternMatch = True Or MatchAndSelect.Count < valueCnt
                    Exit For
                End If
            Next valueIdx
        End If
        If localMS = True And resetFilter = True And Selected = False And Not pItem.Name = "(blank)" Then
            pItem.Selected = False
        End If
    Next pItem
        
    If MatchAndSelect.Count = 0 Then
        If resetFilter = True Then Call Me.SetOnlyBlankItem
    ElseIf localMS = True And blankMatched = False Then Me.BlankItem.Item.Visible = False
    End If
    
End Function

Public Function MatchAndSelectString(matchValueStr As String, patternMatch As Boolean, resetFilter As Boolean) As Collection
    Dim matchValues() As String: matchValues = Split(matchValueStr, FilterStrDelimator)
    
    Set MatchAndSelectString = Me.MatchAndSelect(matchValues, patternMatch, resetFilter)
    
End Function


