# Results
```
goarch=amd64
goos=linux
cpu=AMD Ryzen 7 3700X 8-Core Processor
```
## Performance
### fast_storage/data_similar_to_ssb_messages/append-16
![](./fast_storage-data_similar_to_ssb_messages-append-16.png)
```
         badger_5000 = 28646754 ns per op
    badger_zstd_5000 = 28782914 ns per op
  badger_snappy_5000 = 31076736 ns per op
          bbolt_5000 = 41967810 ns per op
            margaret = 43865683 ns per op
```
### fast_storage/data_similar_to_ssb_messages/read_iterate-16
![](./fast_storage-data_similar_to_ssb_messages-read_iterate-16.png)
```
          bbolt_5000 = 327328 ns per op
  badger_snappy_5000 = 4051823 ns per op
         badger_5000 = 4428383 ns per op
    badger_zstd_5000 = 4456641 ns per op
            margaret = 17499162 ns per op
```
### fast_storage/data_similar_to_ssb_messages/read_random-16
![](./fast_storage-data_similar_to_ssb_messages-read_random-16.png)
```
          bbolt_5000 = 6735268 ns per op
    badger_zstd_5000 = 10399706 ns per op
         badger_5000 = 10486966 ns per op
            margaret = 10613533 ns per op
  badger_snappy_5000 = 10637581 ns per op
```
### fast_storage/data_similar_to_ssb_messages/read_sequential-16
![](./fast_storage-data_similar_to_ssb_messages-read_sequential-16.png)
```
          bbolt_5000 = 4809848 ns per op
    badger_zstd_5000 = 8288769 ns per op
            margaret = 8474751 ns per op
  badger_snappy_5000 = 8511314 ns per op
         badger_5000 = 8676084 ns per op
```
### slow_storage/data_similar_to_ssb_messages/append-16
![](./slow_storage-data_similar_to_ssb_messages-append-16.png)
```
    badger_zstd_5000 = 31802620 ns per op
         badger_5000 = 39912082 ns per op
  badger_snappy_5000 = 40009700 ns per op
            margaret = 81744551 ns per op
          bbolt_5000 = 146447917 ns per op
```
### slow_storage/data_similar_to_ssb_messages/read_iterate-16
![](./slow_storage-data_similar_to_ssb_messages-read_iterate-16.png)
```
          bbolt_5000 = 324516 ns per op
    badger_zstd_5000 = 4070185 ns per op
  badger_snappy_5000 = 4571082 ns per op
         badger_5000 = 4661227 ns per op
            margaret = 44878807 ns per op
```
### slow_storage/data_similar_to_ssb_messages/read_random-16
![](./slow_storage-data_similar_to_ssb_messages-read_random-16.png)
```
          bbolt_5000 = 6439975 ns per op
    badger_zstd_5000 = 10255906 ns per op
         badger_5000 = 11332098 ns per op
  badger_snappy_5000 = 11494571 ns per op
            margaret = 22069008 ns per op
```
### slow_storage/data_similar_to_ssb_messages/read_sequential-16
![](./slow_storage-data_similar_to_ssb_messages-read_sequential-16.png)
```
          bbolt_5000 = 5154795 ns per op
    badger_zstd_5000 = 8432703 ns per op
         badger_5000 = 9092610 ns per op
  badger_snappy_5000 = 9274495 ns per op
            margaret = 20219166 ns per op
```
## Size

Warning: bbolt metrics are not reliable as bbolt grows its file in large increments. Initially the size of the underlying file is multiplied by two and then once it is at above 1 GiB in size 1 GiB is added to it every time the database runs out of space.
### data_similar_to_ssb_messages-16
![](./data_similar_to_ssb_messages-16.png)
```
    badger_zstd_5000 = 246 bytes per op (n=178086)
  badger_snappy_5000 = 316 bytes per op (n=169382)
            margaret = 413 bytes per op (n=178960)
         badger_5000 = 438 bytes per op (n=170884)
          bbolt_5000 = 810 bytes per op (n=254490)
```
