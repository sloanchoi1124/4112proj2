part1:

q4112_hj.c implemented 2 functions:
    uint64_t q4112_run(.....) and void *worker_thread(...).

q4112_run creates worker threads based on the available threads on the machine. 
Each worker thread first hashes the inner tuples. Then worker thread syncs. Then worker thread does hash joins.

coding style: Didn't find "Google style" for c. Used checkpatch instead.

