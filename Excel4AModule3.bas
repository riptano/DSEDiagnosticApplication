Attribute VB_Name = "Module3"
Option Explicit

Function TranslatePivotTypeFilterVar(typePivotFilter As XlPivotFilterType) As Variant

    Select Case typePivotFilter
        Case XlPivotFilterType.xlBefore
            TranslatePivotTypeFilterVar = "xlBefore"
        Case XlPivotFilterType.xlBeforeOrEqualTo
            TranslatePivotTypeFilterVar = "xlBeforeOrEqualTo"
        Case XlPivotFilterType.xlAfter
            TranslatePivotTypeFilterVar = "xlAfter"
        Case XlPivotFilterType.xlAfterOrEqualTo
            TranslatePivotTypeFilterVar = "xlAfterOrEqualTo"
        Case XlPivotFilterType.xlAllDatesInPeriodJanuary
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodJanuary"
        Case XlPivotFilterType.xlAllDatesInPeriodFebruary
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodFebruary"
        Case XlPivotFilterType.xlAllDatesInPeriodMarch
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodMarch"
        Case XlPivotFilterType.xlAllDatesInPeriodApril
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodApril"
        Case XlPivotFilterType.xlAllDatesInPeriodMay
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodMay"
        Case XlPivotFilterType.xlAllDatesInPeriodJune
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodJune"
        Case XlPivotFilterType.xlAllDatesInPeriodJuly
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodJuly"
        Case XlPivotFilterType.xlAllDatesInPeriodAugust
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodAugust"
        Case XlPivotFilterType.xlAllDatesInPeriodSeptember
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodSeptember"
        Case XlPivotFilterType.xlAllDatesInPeriodOctober
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodOctober"
        Case XlPivotFilterType.xlAllDatesInPeriodNovember
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodNovember"
        Case XlPivotFilterType.xlAllDatesInPeriodDecember
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodDecember"
        Case XlPivotFilterType.xlAllDatesInPeriodQuarter1
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodQuarter1"
        Case XlPivotFilterType.xlAllDatesInPeriodQuarter2
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodQuarter2"
        Case XlPivotFilterType.xlAllDatesInPeriodQuarter3
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodQuarter3"
        Case XlPivotFilterType.xlAllDatesInPeriodQuarter4
            TranslatePivotTypeFilterVar = "xlAllDatesInPeriodQuarter4"
        Case XlPivotFilterType.xlBottomCount
            TranslatePivotTypeFilterVar = "xlBottomCount"
        Case XlPivotFilterType.xlBottomPercent
            TranslatePivotTypeFilterVar = "xlBottomPercent"
        Case XlPivotFilterType.xlBottomSum
            TranslatePivotTypeFilterVar = "xlBottomSum"
        Case XlPivotFilterType.xlCaptionBeginsWith
            TranslatePivotTypeFilterVar = "xlCaptionBeginsWith"
        Case XlPivotFilterType.xlCaptionContains
            TranslatePivotTypeFilterVar = "xlCaptionContains"
        Case XlPivotFilterType.xlCaptionDoesNotBeginWith
            TranslatePivotTypeFilterVar = "xlCaptionDoesNotBeginWith"
        Case XlPivotFilterType.xlCaptionDoesNotContain
            TranslatePivotTypeFilterVar = "xlCaptionDoesNotContain"
        Case XlPivotFilterType.xlCaptionDoesNotEndWith
            TranslatePivotTypeFilterVar = "xlCaptionDoesNotEndWith"
        Case XlPivotFilterType.xlCaptionDoesNotEqual
            TranslatePivotTypeFilterVar = "xlCaptionDoesNotEqual"
        Case XlPivotFilterType.xlCaptionEndsWith
            TranslatePivotTypeFilterVar = "xlCaptionEndsWith"
        Case XlPivotFilterType.xlCaptionEquals
            TranslatePivotTypeFilterVar = "xlCaptionEquals"
        Case XlPivotFilterType.xlCaptionIsBetween
            TranslatePivotTypeFilterVar = "xlCaptionIsBetween"
        Case XlPivotFilterType.xlCaptionIsGreaterThan
            TranslatePivotTypeFilterVar = "xlCaptionIsGreaterThan"
        Case XlPivotFilterType.xlCaptionIsGreaterThanOrEqualTo
            TranslatePivotTypeFilterVar = "xlCaptionIsGreaterThanOrEqualTo"
        Case XlPivotFilterType.xlCaptionIsLessThan
            TranslatePivotTypeFilterVar = "xlCaptionIsLessThan"
        Case XlPivotFilterType.xlCaptionIsLessThanOrEqualTo
            TranslatePivotTypeFilterVar = "xlCaptionIsLessThanOrEqualTo"
        Case XlPivotFilterType.xlCaptionIsNotBetween
            TranslatePivotTypeFilterVar = "xlCaptionIsNotBetween"
        Case XlPivotFilterType.xlDateBetween
            TranslatePivotTypeFilterVar = "xlDateBetween"
        Case XlPivotFilterType.xlDateLastMonth
            TranslatePivotTypeFilterVar = "xlDateLastMonth"
        Case XlPivotFilterType.xlDateLastQuarter
            TranslatePivotTypeFilterVar = "xlDateLastQuarter"
        Case XlPivotFilterType.xlDateLastWeek
            TranslatePivotTypeFilterVar = "xlDateLastWeek"
        Case XlPivotFilterType.xlDateLastYear
            TranslatePivotTypeFilterVar = "xlDateLastYear"
        Case XlPivotFilterType.xlDateNextMonth
            TranslatePivotTypeFilterVar = "xlDateNextMonth"
        Case XlPivotFilterType.xlDateNextQuarter
            TranslatePivotTypeFilterVar = "xlDateNextQuarter"
        Case XlPivotFilterType.xlDateNextWeek
            TranslatePivotTypeFilterVar = "xlDateNextWeek"
        Case XlPivotFilterType.xlDateNextYear
            TranslatePivotTypeFilterVar = "xlDateNextYear"
        Case XlPivotFilterType.xlDateThisMonth
            TranslatePivotTypeFilterVar = "xlDateThisMonth"
        Case XlPivotFilterType.xlDateThisQuarter
            TranslatePivotTypeFilterVar = "xlDateThisQuarter"
        Case XlPivotFilterType.xlDateThisWeek
            TranslatePivotTypeFilterVar = "xlDateThisWeek"
        Case XlPivotFilterType.xlDateThisYear
            TranslatePivotTypeFilterVar = "xlDateThisYear"
        Case XlPivotFilterType.xlDateToday
            TranslatePivotTypeFilterVar = "xlDateToday"
        Case XlPivotFilterType.xlDateTomorrow
            TranslatePivotTypeFilterVar = "xlDateTomorrow"
        Case XlPivotFilterType.xlDateYesterday
            TranslatePivotTypeFilterVar = "xlDateYesterday"
        Case XlPivotFilterType.xlNotSpecificDate
            TranslatePivotTypeFilterVar = "xlNotSpecificDate"
        Case XlPivotFilterType.xlSpecificDate
            TranslatePivotTypeFilterVar = "xlSpecificDate"
        Case XlPivotFilterType.xlTopCount
            TranslatePivotTypeFilterVar = "xlTopCount"
        Case XlPivotFilterType.xlTopPercent
            TranslatePivotTypeFilterVar = "xlTopPercent"
        Case XlPivotFilterType.xlTopSum
            TranslatePivotTypeFilterVar = "xlTopSum"
        Case XlPivotFilterType.xlValueDoesNotEqual
            TranslatePivotTypeFilterVar = "xlValueDoesNotEqual"
        Case XlPivotFilterType.xlValueEquals
            TranslatePivotTypeFilterVar = "xlValueEquals"
        Case XlPivotFilterType.xlValueIsBetween
            TranslatePivotTypeFilterVar = "xlValueIsBetween"
        Case XlPivotFilterType.xlValueIsGreaterThan
            TranslatePivotTypeFilterVar = "xlValueIsGreaterThan"
        Case XlPivotFilterType.xlValueIsGreaterThanOrEqualTo
            TranslatePivotTypeFilterVar = "xlValueIsGreaterThanOrEqualTo"
        Case XlPivotFilterType.xlValueIsLessThan
            TranslatePivotTypeFilterVar = "xlValueIsLessThan"
        Case XlPivotFilterType.xlValueIsLessThanOrEqualTo
            TranslatePivotTypeFilterVar = "xlValueIsLessThanOrEqualTo"
        Case XlPivotFilterType.xlValueIsNotBetween
            TranslatePivotTypeFilterVar = "xlValueIsNotBetween"
        Case XlPivotFilterType.xlYearToDate
            TranslatePivotTypeFilterVar = "xlYearToDate"
        Case Else
            TranslatePivotTypeFilterVar = typePivotFilter
    End Select
    
End Function

Function TranslateVarPivotFilterType(typePivotFilter As Variant) As XlPivotFilterType

    Select Case typePivotFilter
        Case "xlBefore"
            TranslateVarPivotFilterType = XlPivotFilterType.xlBefore
        Case "xlBeforeOrEqualTo"
            TranslateVarPivotFilterType = XlPivotFilterType.xlBeforeOrEqualTo
        Case "xlAfter"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAfter
        Case "xlAfterOrEqualTo"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAfterOrEqualTo
        Case "xlAllDatesInPeriodJanuary"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodJanuary
        Case "xlAllDatesInPeriodFebruary"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodFebruary
        Case "xlAllDatesInPeriodMarch"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodMarch
        Case "xlAllDatesInPeriodApril"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodApril
        Case "xlAllDatesInPeriodMay"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodMay
        Case "xlAllDatesInPeriodJune"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodJune
        Case "xlAllDatesInPeriodJuly"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodJuly
        Case "xlAllDatesInPeriodAugust"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodAugust
        Case "xlAllDatesInPeriodSeptember"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodSeptember
        Case "xlAllDatesInPeriodOctober"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodOctober
        Case "xlAllDatesInPeriodNovember"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodNovember
        Case "xlAllDatesInPeriodDecember"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodDecember
        Case "xlAllDatesInPeriodQuarter1"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodQuarter1
        Case "xlAllDatesInPeriodQuarter2"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodQuarter2
        Case "xlAllDatesInPeriodQuarter3"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodQuarter3
        Case "xlAllDatesInPeriodQuarter4"
            TranslateVarPivotFilterType = XlPivotFilterType.xlAllDatesInPeriodQuarter4
        Case "xlBottomCount"
            TranslateVarPivotFilterType = XlPivotFilterType.xlBottomCount
        Case "xlBottomPercent"
            TranslateVarPivotFilterType = XlPivotFilterType.xlBottomPercent
        Case "xlBottomSum"
            TranslateVarPivotFilterType = XlPivotFilterType.xlBottomSum
        Case "xlCaptionBeginsWith"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionBeginsWith
        Case "xlCaptionContains"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionContains
        Case "xlCaptionDoesNotBeginWith"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionDoesNotBeginWith
        Case "xlCaptionDoesNotContain"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionDoesNotContain
        Case "xlCaptionDoesNotEndWith"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionDoesNotEndWith
        Case "xlCaptionDoesNotEqual"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionDoesNotEqual
        Case "xlCaptionEndsWith"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionEndsWith
        Case "xlCaptionEquals"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionEquals
        Case "xlCaptionIsBetween"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionIsBetween
        Case "xlCaptionIsGreaterThan"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionIsGreaterThan
        Case "xlCaptionIsGreaterThanOrEqualTo"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionIsGreaterThanOrEqualTo
        Case "xlCaptionIsLessThan"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionIsLessThan
        Case "xlCaptionIsLessThanOrEqualTo"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionIsLessThanOrEqualTo
        Case "xlCaptionIsNotBetween"
            TranslateVarPivotFilterType = XlPivotFilterType.xlCaptionIsNotBetween
        Case "xlDateBetween"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateBetween
        Case "xlDateLastMonth"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateLastMonth
        Case "xlDateLastQuarter"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateLastQuarter
        Case "xlDateLastWeek"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateLastWeek
        Case "xlDateLastYear"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateLastYear
        Case "xlDateNextMonth"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateNextMonth
        Case "xlDateNextQuarter"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateNextQuarter
        Case "xlDateNextWeek"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateNextWeek
        Case "xlDateNextYear"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateNextYear
        Case "xlDateThisMonth"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateThisMonth
        Case "xlDateThisQuarter"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateThisQuarter
        Case "xlDateThisWeek"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateThisWeek
        Case "xlDateThisYear"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateThisYear
        Case "xlDateToday"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateToday
        Case "xlDateTomorrow"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateTomorrow
        Case "xlDateYesterday"
            TranslateVarPivotFilterType = XlPivotFilterType.xlDateYesterday
        Case "xlNotSpecificDate"
            TranslateVarPivotFilterType = XlPivotFilterType.xlNotSpecificDate
        Case "xlSpecificDate"
            TranslateVarPivotFilterType = XlPivotFilterType.xlSpecificDate
        Case "xlTopCount"
            TranslateVarPivotFilterType = XlPivotFilterType.xlTopCount
        Case "xlTopPercent"
            TranslateVarPivotFilterType = XlPivotFilterType.xlTopPercent
        Case "xlTopSum"
            TranslateVarPivotFilterType = XlPivotFilterType.xlTopSum
        Case "xlValueDoesNotEqual"
            TranslateVarPivotFilterType = XlPivotFilterType.xlValueDoesNotEqual
        Case "xlValueEquals"
            TranslateVarPivotFilterType = XlPivotFilterType.xlValueEquals
        Case "xlValueIsBetween"
            TranslateVarPivotFilterType = XlPivotFilterType.xlValueIsBetween
        Case "xlValueIsGreaterThan"
            TranslateVarPivotFilterType = XlPivotFilterType.xlValueIsGreaterThan
        Case "xlValueIsGreaterThanOrEqualTo"
            TranslateVarPivotFilterType = XlPivotFilterType.xlValueIsGreaterThanOrEqualTo
        Case "xlValueIsLessThan"
            TranslateVarPivotFilterType = XlPivotFilterType.xlValueIsLessThan
        Case "xlValueIsLessThanOrEqualTo"
            TranslateVarPivotFilterType = XlPivotFilterType.xlValueIsLessThanOrEqualTo
        Case "xlValueIsNotBetween"
            TranslateVarPivotFilterType = XlPivotFilterType.xlValueIsNotBetween
        Case "xlYearToDate"
            TranslateVarPivotFilterType = XlPivotFilterType.xlYearToDate
        Case Else
            TranslateVarPivotFilterType = typePivotFilter
    End Select
    
End Function
