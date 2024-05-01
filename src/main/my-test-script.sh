go run mrcoordinator.go pg-* &
timeout -k 2s 120s go run mrworker.go indexer.so &
timeout -k 2s 120s go run mrworker.go indexer.so 
