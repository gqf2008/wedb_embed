### Ubuntu CI (GitHub Actions Runner)

#### 硬件与测试环境

CPU: INTEL(R) XEON(R) PLATINUM 8573C (4核)<br>
内存: 15.6 GB<br>
硬盘: Azure Managed Virtual Disk (Cloud Standard SSD)<br>
系统: Ubuntu 24.04.4 LTS (Linux 6.17.0-1022-azure)<br>
Rust: 1.98.0 (88d9e12ae 2026-08-18)<br>
Redis: v8.10.1

#### 真实物理落盘与内存占用实测 (5GB 数据规模)

| 资源维度 | wedb_embed (嵌入式 LSM+LZ4) | Redis (v8.10.1 AOF持久化) | 资源节省比例 |
| :--- | :--- | :--- | :--- |
| **测试数据规模** | 5,000,000 条全格式结构化数据 | 5,000,000 条全格式结构化数据 | 14 种数据格式等比实测 |
| **原始数据载荷** | 4377 MB | 4377 MB | 真实结构化载荷 |
| **实际物理落盘大小** | **1180 MB** | **7959 MB** | **节省 85%** |
| **进程常驻内存 (RSS)** | **241 MB** | **4878 MB** | **节省 95%** |

#### wedb_embed vs Redis 核心指令性能对比

| 指令 | wedb_embed P95延迟 | Redis P95延迟 | 性能领先 |
| :--- | :--- | :--- | :--- |
| `SET` | 8.3 us | 30.1 us | **3.6x** |
| `GET` | 5.4 us | 29.0 us | **5.3x** |
| `MSET` | 55.6 us | 26.7 us | **0.5x** |
| `MGET` | 5.6 us | 26.3 us | **4.7x** |
| `INCRBY` | 1.1 us | 29.7 us | **27.1x** |
| `DECRBY` | 0.67 us | 30.0 us | **44.6x** |
| `APPEND` | 0.84 us | 29.9 us | **35.7x** |
| `STRLEN` | 0.32 us | 26.3 us | **81.6x** |
| `GETDEL` | 9.1 us | 52.4 us | **5.7x** |
| `GETRANGE` | 0.32 us | 29.0 us | **91.4x** |
| `SETRANGE` | 0.83 us | 26.5 us | **31.9x** |
| `HSET` | 2.0 us | 38.0 us | **19.3x** |
| `HGET` | 0.71 us | 36.3 us | **50.9x** |
| `HMGET` | 3.3 us | 38.3 us | **11.7x** |
| `HEXISTS` | 0.65 us | 35.9 us | **55.1x** |
| `HLEN` | 0.45 us | 35.6 us | **79.0x** |
| `HDEL` | 4.7 us | 36.7 us | **7.8x** |
| `HGETALL` | 3.1 us | 37.7 us | **12.1x** |
| `HKEYS` | 3.0 us | 37.1 us | **12.3x** |
| `HVALS` | 3.1 us | 37.4 us | **11.9x** |
| `HINCRBY` | 1.7 us | 38.7 us | **22.8x** |
| `LPUSH` | 1.9 us | 30.1 us | **15.8x** |
| `RPUSH` | 1.8 us | 30.1 us | **16.3x** |
| `LPOP` | 2.4 us | 33.2 us | **13.8x** |
| `RPOP` | 2.3 us | 26.5 us | **11.3x** |
| `LLEN` | 0.47 us | 35.8 us | **76.8x** |
| `LRANGE` | 3.8 us | 26.1 us | **6.9x** |
| `LINDEX` | 0.70 us | 36.0 us | **51.8x** |
| `LSET` | 1.1 us | 26.1 us | **22.8x** |
| `LREM` | 11.7 us | 52.5 us | **4.5x** |
| `LTRIM` | 1.1 us | 26.3 us | **23.4x** |
| `SADD` | 1.4 us | 26.3 us | **18.4x** |
| `SREM` | 4.2 us | 26.2 us | **6.2x** |
| `SISMEMBER` | 0.74 us | 26.1 us | **35.2x** |
| `SCARD` | 0.47 us | 28.8 us | **61.9x** |
| `SMEMBERS` | 3.1 us | 29.2 us | **9.4x** |
| `SPOP` | 7.2 us | 53.4 us | **7.5x** |
| `SRANDMEMBER` | 3.2 us | 26.1 us | **8.1x** |
| `ZADD` | 2.9 us | 54.3 us | **18.8x** |
| `ZSCORE` | 0.84 us | 26.7 us | **31.7x** |
| `ZRANGE` | 3.6 us | 32.7 us | **9.2x** |
| `ZCARD` | 0.54 us | 48.6 us | **90.8x** |
| `ZCOUNT` | 3.0 us | 49.8 us | **16.6x** |
| `ZINCRBY` | 2.8 us | 67.5 us | **23.8x** |
| `ZRANK` | 3.2 us | 33.1 us | **10.4x** |
| `ZREVRANGE` | 4.9 us | 29.7 us | **6.1x** |
| `ZPOPMIN` | 8.6 us | 122.0 us | **14.2x** |
| `ZREM` | 5.0 us | 36.3 us | **7.3x** |
| `SETBIT` | 14.2 us | 33.0 us | **2.3x** |
| `GETBIT` | 0.47 us | 30.4 us | **64.6x** |
| `BITCOUNT` | 0.58 us | 34.2 us | **58.6x** |
| `BITPOS` | 0.64 us | 28.5 us | **44.6x** |
| `PFADD` | 2.7 us | 36.4 us | **13.5x** |
| `PFCOUNT` | 34.6 us | 35.8 us | **1.0x** |
| `GEOADD` | 2.6 us | 53.4 us | **20.9x** |
| `GEODIST` | 0.96 us | 49.3 us | **51.3x** |
| `GEOPOS` | 0.70 us | 37.1 us | **53.1x** |
| `GEOHASH` | 0.72 us | 38.1 us | **53.0x** |
| `XADD` | 1.8 us | 26.3 us | **14.7x** |
| `XLEN` | 0.62 us | 25.9 us | **41.6x** |
| `XRANGE` | 3.8 us | 26.8 us | **7.1x** |
| `XREAD` | 4.9 us | 26.4 us | **5.4x** |
| `XDEL` | 3.6 us | 61.8 us | **17.0x** |
| `DEL` | 3.7 us | 47.4 us | **12.7x** |
| `EXISTS` | 0.24 us | 48.6 us | **198.5x** |
| `EXPIRE` | 0.78 us | 67.3 us | **86.2x** |
| `TTL` | 0.27 us | 45.5 us | **166.5x** |
| `JSON.SET` | 3.1 us | 37.8 us | **12.0x** |
| `JSON.GET` | 1.3 us | 37.3 us | **28.8x** |
| `JSON.DEL` | 9.4 us | 73.4 us | **7.8x** |
| `JSON.NUMINCRBY` | 3.5 us | 37.1 us | **10.6x** |
| `JSON.ARRLEN` | 1.2 us | 37.0 us | **31.3x** |
| `JSON.TYPE` | 1.2 us | 37.4 us | **30.9x** |
| `BF.ADD` | 11.8 us | 32.6 us | **2.8x** |
| `BF.EXISTS` | 0.67 us | 32.4 us | **48.7x** |
| `BF.INFO` | 0.40 us | 32.8 us | **81.3x** |
| `CF.ADD` | 2.4 us | 49.6 us | **20.8x** |
| `CF.EXISTS` | 0.83 us | 50.3 us | **60.6x** |
| `CF.DEL` | 6.6 us | 99.9 us | **15.2x** |
| `TDIGEST.ADD` | 2.7 us | 29.4 us | **11.0x** |
| `TDIGEST.QUANTILE` | 0.99 us | 50.0 us | **50.3x** |
| `TDIGEST.BYRANK` | 1.1 us | 28.7 us | **27.1x** |
| `TDIGEST.CDF` | 1.2 us | 50.4 us | **43.6x** |
| `TS.ADD` | 11.4 us | 60.5 us | **5.3x** |
| `TS.GET` | 1.3 us | 49.8 us | **36.9x** |
| `TS.RANGE` | 26.1 us | 50.5 us | **1.9x** |
| `TS.INCRBY` | 8.4 us | 50.3 us | **6.0x** |
| `FT.SEARCH` | 20.2 us | 31.0 us | **1.5x** |
| `FT.TAG` | 20.3 us | 30.2 us | **1.5x** |
| `VECTOR.KNN` | 3.5 us | 68.0 us | **19.3x** |

