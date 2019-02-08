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

Public Field As PivotField
Public Name As String
Private fieldItems As Collection
Public pivotType As PivotTypes
Public LabelFilter As PivotFilter
Public ValueFilter As PivotFilter
Private fieldBlankItem As PivotItemInfo
Private fieldAllItem As PivotItemInfo

Const FilterStrDelimator As String = ",. "

Public Property Get Items() As Collection
    If fieldItems Is Nothing Then
        Call Me.PopulateFieldItems
    End If
    Set Items = fieldItems
End Property


Public Property Get BlankItem() As PivotItemInfo

    If fieldBlankItem Is Nothing Then
        Dim pivotItem As pivotItem
        
        Set pivotItem = Me.Field.PivotItems("(blank)")
        Set fieldBlankItem = New PivotItemInfo
        Call fieldBlankItem.SetPivotItem(pivotItem, Me)
    End If

    Set BlankItem = fieldBlankItem
End Property
Public Property Set BlankItem(item As PivotItemInfo)
    Set fieldBlankItem = item
End Property

Public Property Get AllItem() As PivotItemInfo

    If fieldAllItem Is Nothing Then
        Set fieldAllItem = New PivotItemInfo
        Call fieldAllItem.SetAsAllPivotItem(Me)
    End If

    Set AllItem = fieldAllItem

End Property
Public Property Set AllItem(item As PivotItemInfo)
    Set fieldAllItem = item
End Property
Private Sub Class_Initialize()
    
    Me.pivotType = Unknown
    Me.Name = Empty
    
    Set Me.LabelFilter = Nothing
    Set Me.ValueFilter = Nothing
    Set fieldItems = Nothing
    Set fieldBlankItem = Nothing
    Set fieldAllItem = Nothing
    
End Sub

Public Sub SetName(pField As PivotField)
    On Error Resume Next
    
    Dim sSetName As String
    
    sSetName = Trim(pField.SourceName)
    
    If IsEmpty(sSetName) = True Then
        sSetName = Trim(pField.Name)
    End If
         
    Me.Name = sSetName
    
    On Error GoTo 0
    
End Sub

Public Function GetItemName(pItem As pivotItem) As String
    On Error Resume Next
    
    GetItemName = Trim(pItem.SourceName)
    
    If IsEmpty(GetItemName) = True Then
        GetItemName = Trim(pItem.Caption)
    End If
     
    On Error GoTo 0
    
End Function
Public Function SetPivotField(pField As PivotField, pType As PivotTypes, Optional populateItems As Boolean = False) As PivotFieldrInfo

    Set SetPivotField = Me
    
    Set Me.Field = pField
    Me.pivotType = pType
    Call Me.SetName(pField)
        
    If Not pField.pivotFilters Is Nothing And pField.pivotFilters.Count = 1 Then
        If pField.pivotFilters(1).Active = True Then
            If pField.pivotFilters(1).Order = -1 Then
                Set Me.ValueFilter = pField.pivotFilters(1)
            Else
                Set Me.LabelFilter = pField.pivotFilters(1)
            End If
        End If
    End If
       
    If populateItems = True Then
        Call Me.PopulateFieldItems
    Else
        Set fieldItems = Nothing
    End If

End Function

Public Function PopulateFieldItems() As PivotFieldrInfo

    Set PopulateFieldItems = Me
    
    Dim pItem As pivotItem
    Dim pItemInfo As PivotItemInfo

    Set fieldItems = New Collection
    
    For Each pItem In Me.Field.PivotItems
        
        Set pItemInfo = New PivotItemInfo
        Call pItemInfo.SetPivotItem(pItem, Me)
        fieldItems.Add item:=pItemInfo, Key:=pItemInfo.Name
        
        If pItemInfo.IsAll Then
            Set Me.AllItem = pItemInfo
        ElseIf pItemInfo.IsBlank Then
            Set Me.BlankItem = pItemInfo
        End If
        
    Next pItem

End Function

Public Property Get MultiSelection() As Boolean
    If Me.pivotType = PageType Then
        MultiSelection = Me.Field.EnableMultiplePageItems = True
    Else
        MultiSelection = True
    End If
End Property

Public Property Get Selected() As Collection
    Dim pItem As PivotItemInfo
    
    Set Selected = New Collection
    
    If Me.MultiSelection = True Then
        'Dim blankSelected As Boolean: blankSelected = False
        For Each pItem In Me.Items
            If pItem.Selected = True Then
                Selected.Add item:=pItem, Key:=pItem.Name
                'If pItem.IsBlank Then blankSelected = True
            End If
        Next pItem
        If Me.Items.Count = Selected.Count Then 'IIf(blankSelected = True, Selected.Count, Selected.Count + 1)
            Set Selected = Nothing
            Set Selected = New Collection
            Selected.Add item:=Me.AllItem, Key:=Me.AllItem.Name
        End If
    ElseIf Me.Field.CurrentPage = "(All)" Then
        Selected.Add item:=Me.AllItem, Key:=Me.AllItem.Name
    ElseIf Me.Field.CurrentPage = "(blank)" Then
        Selected.Add item:=Me.BlankItem, Key:=Me.BlankItem.Name
    Else
        For Each pItem In Me.Items
            If pItem.item.Caption = Me.Field.CurrentPage Then
                Selected.Add item:=pItem, Key:=pItem.Name
                Exit For
            End If
        Next pItem
    End If
    
End Property

Public Property Get PivotTypeString() As String
    
    Select Case Me.pivotType
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
            Me.pivotType = PageType
        Case "Column"
            Me.pivotType = ColumnType
        Case "Row"
            Me.pivotType = RowType
        Case Else
            Me.pivotType = Unknown
    End Select
    
End Property

Public Property Get IsExpanded() As Boolean
    On Error GoTo Done
    
    IsExpanded = False
    
    If Not Me.pivotType = RowType Then Exit Property
    
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
    
    If Not Me.pivotType = RowType Then Exit Property
    
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
    
    If Not Me.pivotType = RowType Then Exit Function
    
    If IsNumeric(Me.Field.Position) Then
        'If Not Me.Field.ShowDetail = expandField Then
            Me.Field.ShowDetail = expandField
        'End If
    End If
    
Done:

     On Error GoTo 0
    
End Function

Public Property Get SelectedValues() As Collection
    Dim pItem As PivotItemInfo
    
    Set SelectedValues = New Collection
    
    If Me.MultiSelection = True Then
        'Dim blankSelected As Boolean: blankSelected = False
        For Each pItem In Me.Items
            If pItem.Selected = True Then
                SelectedValues.Add pItem.Name
                'If pItem.IsBlank Then blankSelected = True
            End If
        Next pItem
        If Me.Items.Count = Selected.Count Then 'IIf(blankSelected = True, Selected.Count, Selected.Count + 1)
            Set SelectedValues = Nothing
            Set SelectedValues = New Collection
            SelectedValues.Add Me.AllItem.Name
        End If
    ElseIf Me.Field.CurrentPage = "(All)" Then
        SelectedValues.Add Me.AllItem.Name
    ElseIf Me.Field.CurrentPage = "(blank)" Then
        SelectedValues.Add Me.BlankItem.Name
    Else
        For Each pItem In Me.Items
            If pItem.item.Caption = Me.Field.CurrentPage Then
                SelectedValues.Add pItem.Name
                Exit For
            End If
        Next pItem
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

Public Function DeSelectAll() As PivotFieldrInfo
    Set DeSelectAll = Me
        
    If Me.MultiSelection = True Then
        Dim pfItem As PivotItemInfo
        Me.BlankItem.item.Visible = True
        For Each pfItem In Me.Items
            If pfItem.IsBlank = False Then
                pfItem.Selected = False
            End If
        Next pfItem
    Else
        Me.BlankItem.Selected = True
    End If
    
End Function

Public Function SelectBlank() As PivotFieldrInfo

    Set SelectBlank = Me.DeSelectAll
    
End Function

Public Function SelectAll() As PivotFieldrInfo
    Set SelectAll = Me
      
    Me.Field.ClearManualFilter
        
End Function

Public Function Matched(matchValues() As String, patternMatch As Boolean) As Collection
    Dim value As String
    Dim valueLBnd As Long: valueLBnd = LBound(matchValues)
    Dim valueUBnd As Long: valueUBnd = UBound(matchValues)
    Dim valueCnt As Long: valueCnt = valueUBnd - valueLBnd + 1
    Dim valueIdx As Long
    Dim pItemInfo As PivotItemInfo
    
    Set Matched = New Collection
    
    If valueCnt = 0 Then Exit Function
    If valueCnt = 1 And matchValues(valueLBnd) = "(All)" Then
        Matched.Add Me.AllItem
    ElseIf fieldItems Is Nothing Then
        Dim pItem As pivotItem
    
        If patternMatch = True Then
            For Each pItem In Me.Field.PivotItems
                For valueIdx = valueLBnd To valueUBnd
                    If GetItemName(pItem) Like matchValues(valueIdx) Then
                        Set pItemInfo = New PivotItemInfo
                        Call pItemInfo.SetPivotItem(pItem, Me)
                        Matched.Add pItemInfo
                        Exit For
                    End If
                Next valueIdx
            Next pItem
        Else
            On Error Resume Next
            For valueIdx = valueLBnd To valueUBnd
                Set pItem = Nothing
                Set pItem = Me.Field.PivotItems(matchValues(valueIdx))
                
                If Not pItem Is Nothing Then
                    Set pItemInfo = New PivotItemInfo
                    Call pItemInfo.SetPivotItem(pItem, Me)
                    Matched.Add pItemInfo
                    If Matched.Count = valueCnt Then Exit Function
                End If
            Next valueIdx
            On Error GoTo 0
        End If
    Else
        For Each pItemInfo In Me.Items
            For valueIdx = valueLBnd To valueUBnd
                If IIf(patternMatch = True, pItemInfo.Name Like matchValues(valueIdx), matchValues(valueIdx) = pItemInfo.Name) Then
                    Matched.Add pItemInfo
                    If patternMatch = False And Matched.Count = valueCnt Then Exit Function
                    Exit For
                End If
            Next valueIdx
        Next pItemInfo
    End If
    
End Function

Public Function MatchAndSelect(matchValues() As String, patternMatch As Boolean, resetFilter As Boolean) As Collection
    Dim value As String
    Dim pItemInfo As PivotItemInfo
    Dim valueLBnd As Long: valueLBnd = LBound(matchValues)
    Dim valueUBnd As Long: valueUBnd = UBound(matchValues)
    Dim valueCnt As Long: valueCnt = valueUBnd - valueLBnd + 1
    Dim valueIdx As Long
    
    Set MatchAndSelect = New Collection
    
    If valueCnt = 0 Then Exit Function
    
    If valueCnt = 1 Then
        If matchValues(valueLBnd) = "(All)" Then
            Call Me.SelectAll
            MatchAndSelect.Add Me.AllItem
            Exit Function
        ElseIf matchValues(valueLBnd) = "(blank)" Then
            Call Me.SelectBlank
            MatchAndSelect.Add Me.BlankItem
            Exit Function
        End If
    End If
    
    Dim blankMatched As Boolean
    Dim localMS As Boolean: localMS = Me.MultiSelection
     
    If localMS = True Then
        If Me.BlankItem.item.Visible = False Then
            Me.BlankItem.item.Visible = True
        ElseIf resetFilter = False Then
            blankMatched = True
        End If
    Else
        For valueIdx = valueLBnd To valueUBnd
            If IIf(patternMatch = True, Me.Field.CurrentPage Like matchValues(valueIdx), matchValues(valueIdx) = Me.Field.CurrentPage) Then
                Set MatchAndSelect = Me.Selected
                Exit Function
            End If
        Next valueIdx
    End If
    
    Dim vSelected As Boolean
    Dim checkValues As Boolean: checkValues = True
   
    If fieldItems Is Nothing Then
        Dim pItem As pivotItem
        Dim pItemName As String
        
        For Each pItem In Me.Field.PivotItems
            vSelected = False
            pItemName = Me.GetItemName(pItem)
            
            If checkValues = True Then
                For valueIdx = valueLBnd To valueUBnd
                    If IIf(patternMatch = True, pItemName Like matchValues(valueIdx), matchValues(valueIdx) = pItemName) Then
                        Set pItemInfo = New PivotItemInfo
                        Call pItemInfo.SetPivotItem(pItem, Me)
                        MatchAndSelect.Add pItemInfo
                        pItemInfo.Selected = True
                        If localMS = False Then Exit Function
                        vSelected = True
                        If pItemInfo.Name = "(blank)" Then blankMatched = True
                        checkValues = patternMatch = True Or MatchAndSelect.Count < valueCnt
                        Exit For
                    End If
                Next valueIdx
            End If
            If localMS = True And resetFilter = True And vSelected = False And Not pItemName = "(blank)" Then
                pItem.Visible = False
            End If
        Next pItem
    Else
        For Each pItemInfo In Me.Items
            vSelected = False
            If checkValues = True Then
                For valueIdx = valueLBnd To valueUBnd
                    If IIf(patternMatch = True, pItemInfo.Name Like matchValues(valueIdx), matchValues(valueIdx) = pItemInfo.Name) Then
                        MatchAndSelect.Add pItemInfo
                        pItemInfo.Selected = True
                        If localMS = False Then Exit Function
                        vSelected = True
                        If pItemInfo.Name = "(blank)" Then blankMatched = True
                        checkValues = patternMatch = True Or MatchAndSelect.Count < valueCnt
                        Exit For
                    End If
                Next valueIdx
            End If
            If localMS = True And resetFilter = True And vSelected = False And Not pItemInfo.Name = "(blank)" Then
                pItemInfo.Selected = False
            End If
        Next pItemInfo
    End If
    
    If MatchAndSelect.Count = 0 Then
        If resetFilter = True Then Call Me.SelectBlank
    ElseIf localMS = True And blankMatched = False Then Me.BlankItem.item.Visible = False
    End If
    
End Function

Public Function MatchAndSelectString(matchValueStr As String, patternMatch As Boolean, resetFilter As Boolean) As Collection
    Dim matchValues() As String: matchValues = Split(matchValueStr, FilterStrDelimator)
    
    Set MatchAndSelectString = Me.MatchAndSelect(matchValues, patternMatch, resetFilter)
    
End Function


