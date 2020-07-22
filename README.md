# PySparkRazor
A simple ETL(pySpark backed) tool for generating features for recommendation backed by pySpark. 

ETL jobs was abstracted as several paradigms and each paradigm are executed by operations. This idea originates 
from [MeiTuan Tech](https://tech.meituan.com/2016/12/09/feature-pipeline.html). They did not open source. 

Following their idea, I implement this ETL tool based on pySpark.

## Features

1. ETL jobs was abstracted as several paradigms and each paradigm are executed by operations
2. Support custom operations
3. Support configure to define specified features

## Usage
Default config json file
```json
{
  "Group": {
    "Column": "image_id"
  },
  "Decay": {
    "Column": "timestamp",
    "EndDate": "20200712",
    "Finish": 0.8
  },
  "Dimensions": [
    {
      "DimValue": {
        "Column": "user_id"
      },
      "Features": [
        {
          "Name": "item_click_sum_14",
          "Column": "click",
          "StatOp": "sum14",
          "AggOp": "last",
          "TransOp": {
            "Op": "scaler_min_max",
            "Args": {
              "min": 0,
              "max": 100
            }
          }
        },
        {
          "Name": "item_browse_sum_14",
          "Column": "browse",
          "StatOp": "sum14",
          "AggOp": "last",
          "TransOp": {
            "Op": "scaler_min_max",
            "Args": {
              "min": 0,
              "max": 100
            }
          }
        }
      ]
    }
  ]
}
```

```python
path = "/yinyajun/tmp/tmp_dat"
file = "./config/image_second.json"

df = sqlContext.read.parquet(path)

fg =FeatureGenerator()
features = fg.transform_aggregated(file, df)
features.show()
```

## Supported Operations
```bash
Supported Ops:

 TransOp
     normalization_norm
         NormalizationNormTransOp
     default
         DefaultTransOp
     bucket
         BucketTransOp
     scaler_min_max
         ScalerMinMaxTransOp
     array_len
         ArrayLenTransOp
     scaler_zscore
         ScalerZscoreTransOp
     str_contain
         StrContainTransOp
     normalization_account
         NormalizationAccountTransOp
     identity
         IdentityTransOp
 StatOp
     default
         DefaultStatOp
     sum5
         Sum5StatOp
     sum30
         Sum30StatOp
     sum14
         Sum14StatOp
     hist
         HistStatOp
     sum_period
         SumPeriodStatOp
     identity
         IdentityStatOp
 AggOp
     default
         DefaultAggOp
     max
         MaxAggOp
     sum
         SumAggOp
     last
         LastAggOp
     first
         FirstAggOp

```

## Config Example
```bash
Current Config: 

 Group
     Column
         image_id
 Dimensions
    --------------------
     DimValue
         Column
             user_id
     Features
        --------------------
         Column
             click
         StatOp
             sum14
         TransOp
             Args
                 max
                     100
                 min
                     0
             Op
                 scaler_min_max
         Name
             item_click_sum_14
         AggOp
             last
        --------------------
        --------------------
         Column
             browse
         StatOp
             sum14
         TransOp
             Args
                 max
                     100
                 min
                     0
             Op
                 scaler_min_max
         Name
             item_browse_sum_14
         AggOp
             last
        --------------------
    --------------------
 Decay
     Column
         timestamp
     Finish
         0.8
     EndDate
         20200712

```

