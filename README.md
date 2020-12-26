# PySparkUtils
Code snippets for PySpark
The Code can be used as a base for starting a PySpark ETL Project. The code is built in such a way that it processes the data at given path in hdfs using the transformation rules and stores the data in hdfs.

## Code Structure 
```
pysparkutils
├── __init__.py
├── dependencies
│   ├── __init__.py
│   ├── file_system_utility.py
│   ├── hocon_config_parser.py
│   ├── logger.py
│   └── utils.py
├── process_data.py
├── process_transform_rules.py
└── transform_rules
    ├── __init__.py
    └── transform_rule_1.py

5 directories, 21 files
```

The code uses HOCON based configuration file for setting the appname, master etc...
dependencies directory will contain the basic utility scripts to get spark session, logger, config_parsers etc..

