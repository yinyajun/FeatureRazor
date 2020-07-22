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
    "Finish": 1.0
  },
  "Dimensions": [
    {
      "DimValue": {
        "Column": "user_id"
      },
      "PrimaryFeatures": [
        {
          "Name": "image_click_sum_5",
          "Column": "click",
          "StatOp": "sum5",
          "AggOp": "last"
        },
        {
          "Name": "image_browse_sum_5",
          "Column": "browse",
          "StatOp": "sum5",
          "AggOp": "last"
        }
      ],
      "CompositeFeatures": [
        {
          "Name": "image_click_sum_5_scaled",
          "PrimaryFeature": "image_click_sum_5",
          "TransOp": {
            "Op": "scaler_min_max",
            "Args": {
              "_min": 0,
              "_max": 100
            }
          }
        },
        {
          "Name": "image_click_hotness_5",
          "PrimaryFeature": [
            "image_click_sum_5",
            "image_browse_sum_5"
          ],
          "TransOp": "save_divide"
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
Config Parser Backend is [json]

Supported Ops:

 TransOp
     normalization_norm
         NormalizationNormTransOp
     default
         DefaultTransOp
     save_divide
         SaveDivideTransOp
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



