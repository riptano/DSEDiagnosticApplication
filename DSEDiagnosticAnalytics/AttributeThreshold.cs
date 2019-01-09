using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticAnalytics
{
    public sealed class AttributeThresholdValue
    {
        [Newtonsoft.Json.JsonConstructor]
        public AttributeThresholdValue(Types type, CalcTypes calctype, decimal threshold, decimal weightFactor, decimal penaltyFactor = 0, decimal weight = 1)
        {
            this.Type = type;
            this.CalcType = calctype;
            this.Threshold = threshold;
            this.WeightFactor = weightFactor;
            this.Weight = weight;
            this.PenaltyFactor = penaltyFactor;
        }

        public enum Types
        {
            Normal = 0,
            Max = 1,
            Min = 2,
            Avg = 3,
            StdDev = 4
        }

        [Flags]
        public enum CalcTypes
        {
            /// <summary>
            /// Threshold calculated as a percentage and compared and weighted as a percentage
            /// </summary>
            ThresholdPercentage = 0x0001,
            /// <summary>
            /// Threshold is treated as a raw value (not calculated as a percentage) but weight is still calculated as a percentage
            /// </summary>
            RawThreshold = 0x0002,
            /// <summary>
            /// Over threshold
            /// </summary>
            Overage = 0x0100,
            /// <summary>
            /// Under threshold
            /// </summary>
            Underage = 0x0200,

            /// <summary>
            /// Penalty and weight calculated when threshold is exceeded 
            /// </summary>
            Normal = ThresholdPercentage | Overage,
            /// <summary>
            /// Penalty and weight calculated when threshold is not exceeded 
            /// </summary>
            UnderThreshold = ThresholdPercentage | Underage
        }

        public Types Type { get; } = Types.Normal;
        public CalcTypes CalcType { get; } = CalcTypes.Normal;

        /// <summary>
        /// The value used to determine if the check value exceeds. 
        /// If not over threshold, the percent of the check value to threshold value is calculated and applied to the weight factor and weight.
        /// If over, the penalty factory is applied to the percent of the check value to threshold and that is added to the weight factor and applied to the weight.
        /// </summary>
        public decimal Threshold { get; }
        public decimal Weight { get; } = 1;

        /// <summary>
        /// The factor applied to the threshold percent and then to the weight.
        /// </summary>
        public decimal WeightFactor { get; }

        /// <summary>
        /// If zero, the penalty factor is not applied. it will just be the weight factor applied to the weight.
        /// </summary>
        public decimal PenaltyFactor { get; } = 0;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="checkValue"></param>
        /// <returns></returns>
        /// <example>
        /// CalcType = Normal
        /// Threshold: 1.5
        /// Weight: 1 (default)
        /// WeightFactor: 0.25
        /// 
        /// PenaltyFactor: 0 (default)
        /// 
        /// checkValue: 1.0 => ((1.0/1.5)*0.25*1) => 0.166
        /// checkValue: 1.5 => (0.25*1) => 0.25
        /// checkValue: 1.9 => (0.25*1) => 0.25
        /// 
        /// New PenaltyFactor: 0.50
        /// 
        /// checkValue: 1.0 => ((1.0/1.5)*0.25*1) => 0.166 
        /// checkValue: 1.5 => ((((1.5/1.5)*0.50)+0.25)*1) => 0.75
        /// checkValue: 1.9 => ((((1.9/1.5)*0.50)+0.25)*1) => 0.883
        /// 
        /// New PenaltyFactor: 1.0
        /// 
        /// checkValue: 1.0 => ((1.0/1.5)*0.25*1) => 0.166 
        /// checkValue: 1.5 => ((((1.5/1.5)*1.0)+0.25)*1) => 1.25
        /// checkValue: 1.9 => ((((1.9/1.5)*1.0)+0.25)*1) => 1.516
        /// 
        /// </example>
        public decimal DetermineWeightFactor(decimal checkValue)
        {
            if ((this.CalcType & CalcTypes.RawThreshold) == CalcTypes.RawThreshold) return this.DetermineWeightFactorRaw(checkValue);

            var pctThreshold = checkValue / this.Threshold;

            if ((this.CalcType & CalcTypes.Overage) == CalcTypes.Overage)
            {
                if (pctThreshold < 1m)
                {
                    return this.WeightFactor * pctThreshold * this.Weight;
                }                
            }
            else
            {
                if (pctThreshold > 1m)
                {
                    return this.WeightFactor * pctThreshold * this.Weight;
                }
            }

            return this.PenaltyFactor == 0
                        ? this.WeightFactor * this.Weight
                        : ((pctThreshold * this.PenaltyFactor) + this.WeightFactor) * this.Weight;
        }

        public decimal DetermineWeightFactorRaw(decimal checkValue)
        {
            var pctThreshold = checkValue / this.Threshold;

            if ((this.CalcType & CalcTypes.Overage) == CalcTypes.Overage)
            {
                if (checkValue > this.Threshold)
                {
                    return this.WeightFactor * pctThreshold * this.Weight;
                }
            }
            else
            {
                if (checkValue < this.Threshold)
                {
                    return this.WeightFactor * pctThreshold * this.Weight;
                }
            }

            return this.PenaltyFactor == 0
                        ? this.WeightFactor * this.Weight
                        : ((pctThreshold * this.PenaltyFactor) + this.WeightFactor) * this.Weight;
        }
    }

    public sealed class AttributeThreshold
    {
        public AttributeThreshold()
        {
            this.AttrName = string.Empty;
        }

        public AttributeThreshold(string attrName)
        {
            this.AttrName = attrName;
        }

        public AttributeThreshold(string attrName, AttributeThresholdValue attrValue, decimal minTrigger)
            : this(attrName)
        {
            this.AttrValue = attrValue;
            this.MinTrigger = minTrigger;
        }

        public AttributeThreshold(string attrName, decimal minTrigger)
            : this(attrName)
        {
            this.MinTrigger = minTrigger;
        }

        public AttributeThreshold(string attrName, AttributeThresholdValue maxValue,
                                                    AttributeThresholdValue minValue,
                                                    decimal minTrigger)
            : this(attrName, minTrigger)
        {
            this.MinValue = minValue;
            this.MaxValue = maxValue;
        }

        public AttributeThreshold(string attrName, AttributeThresholdValue maxValue,
                                                    AttributeThresholdValue minValue,
                                                    AttributeThresholdValue avgValue,
                                                    decimal minTrigger)
            : this(attrName, maxValue, minValue, minTrigger)
        {
            this.AvgValue = avgValue;
        }

        [Newtonsoft.Json.JsonConstructor]
        public AttributeThreshold(string attrName, AttributeThresholdValue maxValue,
                                                    AttributeThresholdValue minValue,
                                                    AttributeThresholdValue avgValue,
                                                    AttributeThresholdValue stddevValue,
                                                    decimal minTrigger)
            : this(attrName, maxValue, minValue, avgValue, minTrigger)
        {
            this.StdDevValue = stddevValue;
        }

        public string AttrName { get; }

        public decimal MinTrigger { get; }

        public AttributeThresholdValue AttrValue { get; }

        public AttributeThresholdValue MaxValue { get; }

        public AttributeThresholdValue MinValue { get; }
        public AttributeThresholdValue AvgValue { get; }

        public AttributeThresholdValue StdDevValue { get; }

        public decimal DetermineWeightFactor(decimal checkValue)
        {
            return this.AttrValue?.DetermineWeightFactor(checkValue) ?? (this.AvgValue?.DetermineWeightFactor(checkValue) ?? 0m);
        }

        public decimal DetermineWeightFactor(decimal maxValue, decimal minValue)
        {
            if (maxValue == minValue)
                return this.AttrValue?.DetermineWeightFactor(maxValue) ?? (this.AvgValue?.DetermineWeightFactor(maxValue) ?? 0m);

            return (this.MaxValue?.DetermineWeightFactor(maxValue) ?? 0m)
                    + (this.MinValue?.DetermineWeightFactor(minValue) ?? 0m);
        }

        public decimal DetermineWeightFactor(decimal maxValue, decimal minValue, decimal avgValue)
        {
            if (maxValue == minValue && minValue == avgValue)
                return this.AttrValue?.DetermineWeightFactor(avgValue) ?? (this.AvgValue?.DetermineWeightFactor(avgValue) ?? 0m);

            return (this.AttrValue?.DetermineWeightFactor(maxValue) ?? 0m)
                    + (this.MinValue?.DetermineWeightFactor(minValue) ?? 0m)
                    + (this.AvgValue?.DetermineWeightFactor(avgValue) ?? 0m);
        }

        public decimal DetermineWeightFactor(decimal maxValue, decimal minValue, decimal avgValue, decimal stddevValue)
        {
            if (maxValue == minValue && minValue == avgValue)
                return (this.AttrValue?.DetermineWeightFactor(avgValue) ?? (this.AvgValue?.DetermineWeightFactor(avgValue) ?? 0m))
                         + (this.StdDevValue?.DetermineWeightFactor(stddevValue) ?? 0m);

            return (this.AttrValue?.DetermineWeightFactor(maxValue) ?? 0m)
                    + (this.MinValue?.DetermineWeightFactor(minValue) ?? 0m)
                    + (this.AvgValue?.DetermineWeightFactor(avgValue) ?? 0m)
                    + (this.StdDevValue?.DetermineWeightFactor(stddevValue) ?? 0m);
        }

        public decimal DetermineWeightFactor(decimal checkValue, AttributeThresholdValue.Types type)
        {
            switch (type)
            {
                case AttributeThresholdValue.Types.Normal:
                    return this.AttrValue?.DetermineWeightFactor(checkValue) ?? 0m;
                case AttributeThresholdValue.Types.Max:
                    return this.MaxValue?.DetermineWeightFactor(checkValue) ?? 0m;
                case AttributeThresholdValue.Types.Min:
                    return this.MinValue?.DetermineWeightFactor(checkValue) ?? 0m;
                case AttributeThresholdValue.Types.Avg:
                    return this.AvgValue?.DetermineWeightFactor(checkValue) ?? 0m;
                case AttributeThresholdValue.Types.StdDev:
                    return this.StdDevValue?.DetermineWeightFactor(checkValue) ?? 0m;
                default:
                    break;
            }
            return 0m;
        }

        public bool DetermineWeightFactor(decimal checkValue, out decimal weightFactor)
        {
            return (weightFactor = this.DetermineWeightFactor(checkValue)) >= this.MinTrigger;
        }

        public bool DetermineWeightFactor(decimal maxValue, decimal minValue, out decimal weightFactor)
        {
            return (weightFactor = this.DetermineWeightFactor(maxValue, minValue)) >= this.MinTrigger;
        }

        public bool DetermineWeightFactor(decimal maxValue, decimal minValue, decimal avgValue, out decimal weightFactor)
        {
            return (weightFactor = this.DetermineWeightFactor(maxValue, minValue, avgValue)) >= this.MinTrigger;
        }

        public bool DetermineWeightFactor(decimal maxValue, decimal minValue, decimal avgValue, decimal stddevValue, out decimal weightFactor)
        {
            return (weightFactor = this.DetermineWeightFactor(maxValue, minValue, avgValue, stddevValue)) >= this.MinTrigger;
        }

        public bool DetermineWeightFactor(decimal checkValue, AttributeThresholdValue.Types type, out decimal weightFactor)
        {
            return (weightFactor = this.DetermineWeightFactor(checkValue, type)) >= this.MinTrigger;
        }

        public readonly static AttributeThreshold Empty = new AttributeThreshold();

        public readonly static AttributeThreshold[] AttributeThresholds = DSEDiagnosticFileParser.LibrarySettings.ReadJsonFileIntoObject<AttributeThreshold[]>(Properties.Settings.Default.AttributeThresholds);

        public static AttributeThreshold FindAttrThreshold(string attrName, bool ifnotFoundEmpty = true)
        {
            var attrThreshold = AttributeThresholds.FirstOrDefault(i => i.AttrName == attrName);

            return attrThreshold ?? (ifnotFoundEmpty ? Empty : null);
        }
    }
}
