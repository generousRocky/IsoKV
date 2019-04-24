<div class="header" align="center">
	<h2>
		<a href="https://rockylim92.github.io/" title="temp">
			<img alt="" src="https://github.com/RockyLim92/copyBox/blob/master/asset/rocky_icon.png" width="100px" height="100px" />
		</a>
		<br />
		DapDB
	</h2>
	<p align="center">:octocat: A LSM-tree based key-value store tailored for Open-Channel SSDs. It based on RocksDB :octocat:</p>
</div>

---

### Overview
Abstract—Modern data centers aim to take advantage of high parallelism in storage devices for I/O intensive applications such as storage servers, cache systems, and database systems. Database systems are the most typical applications that should provide a highly reliable service with high-performance. Therefore, many data centers running database systems are actively introducing next-generation high-performance storage devices such as Non Volatile Memory Express (NVMe) based Solid State Devices (SSDs). NVMe SSDs and its protocol are characterized by taking full advantage of the high degree of parallelism of the device which is expected to ensure enhanced performance of the applications. However, taking full advantage of the device’s parallelism does not always guarantee high performance as well as predictable performance. In particular, heavily mixed read and write requests give rise to serious performance degradation on throughput and response time due to the interferences of requests in SSDs each other. To eliminate the interference in SSDs and improve performance, this paper presents DapDB, a low-latency Key-Value Store tailored for Open-Channel SSD with dynamic arrangement of internal parallelism in SSDs. We divided and isolated entire parallel units (LUNs) of the NVMe SSD into tree type and re-arrange them to three different types of LSM-tree base Key-value store. To implement DapDB based on RocksDB, we used Open-Channel SSD. We modiﬁed storage backend of RocksDB to run application-driven Flash management scheme using Open-Channel SSD. DapDB can fully control internal parallelism of SSD. It may take advantage of entire parallelism for every I/O request, It can use the entire parallelism of the SSD for every I/O request, or it can use a strategy to yield the degree of parallelism of the I/O request to reduce the interference between the various I/O requests. Our extensive experiments have shown that DapDB’s Isolation-Arrangement scheme achieves both improved overall throughput and response time, i.e., on average 1.20× faster and 43% less than traditional Striping-Arrangement scheme respectively.


### Contributor(s)
- **Rocky Lim** - [GitHub](https://github.com/RockyLim92)


### Development
- It based on RocksDB.

#### branch description

* dapdb\_striping: for baseline, all I/O types share LUNs
* dapdb\_isolation: Isolation-Arrangement scheme, each I/O type has dedicated LUNs
* dapdb\_dynamic: Isolation-Arrangement scheme + dynamic arrangement.

### Build (or Installation)
Follow the instructions on [https://github.com/RockyLim92/rocksdb/blob/master/INSTALL.md].

After installation of prerequisite, just type following on 'nvm' directory.

```
~$ sudo make rocks
```


### Usage  
To run db\_bench, type
```
~$ source run.sh
```

### Contents
- `nvm/*`: storage backend for DapDB tailored for Open-Channel SSDs.


### Acknowledgments
- **Matias Bjørling**
- **Javier Gonzalez**
- **DCSLAB, SNU**

### References
- [**DapDB: Cutting Latency in Key-Value Store with Dynamic Arrangement of internal Parallelism in SSD**](https://rockylim92.github.io/publication/DapDB.pdf) - (in progress)
- etc.


* * *

[![Build Status](https://travis-ci.org/facebook/rocksdb.svg?branch=master)](https://travis-ci.org/facebook/rocksdb)

RocksDB is developed and maintained by Facebook Database Engineering Team.
It is built on earlier work on LevelDB by Sanjay Ghemawat (sanjay@google.com)
and Jeff Dean (jeff@google.com)

This code is a library that forms the core building block for a fast
key value server, especially suited for storing data on flash drives.
It has a Log-Structured-Merge-Database (LSM) design with flexible tradeoffs
between Write-Amplification-Factor (WAF), Read-Amplification-Factor (RAF)
and Space-Amplification-Factor (SAF). It has multi-threaded compactions,
making it specially suitable for storing multiple terabytes of data in a
single database.

Start with example usage here: https://github.com/facebook/rocksdb/tree/master/examples

See the [github wiki](https://github.com/facebook/rocksdb/wiki) for more explanation.
Compile for LightNVM using make ENV=NVM.

The public interface is in `include/`.  Callers should not include or
rely on the details of any other header files in this package.  Those
internal APIs may be changed without warning.

Design discussions are conducted in https://www.facebook.com/groups/rocksdb.dev/

For more information about LightNVM integration with RocksDB and collaboration,
please contact Javier González at <jg@lightnvm.io>

