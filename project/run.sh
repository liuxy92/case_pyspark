#!/bin/bash

# start process data with spark
spark-submit analyst.py
# start visualization service
spark-submit web.py