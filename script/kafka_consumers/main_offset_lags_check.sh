#!/bin/bash
sh exec_offset_lags_check.sh candles-minute-consumer
sh exec_offset_lags_check.sh market-trade-consumer
sh exec_offset_lags_check.sh order-book-consumer
