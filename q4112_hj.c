#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>
#include <stdio.h>

#define BIG_NUMBER 0x9e3779b1
#define LOCAL_CACHE_ENABLED 1

typedef struct {
	uint32_t aggr_key;
	uint64_t sum;
	uint32_t count;

} aggr_bucket_t;
pthread_barrier_t inner_table_barrier;
pthread_barrier_t global_hash_barrier;
pthread_barrier_t global_table_creation;
pthread_barrier_t aggr_barrier;

aggr_bucket_t *global_table = NULL;
int8_t log_global_buckets = 0;
size_t global_buckets = 0;
int8_t log_local_buckets = 10;
size_t local_buckets = 1024;

typedef struct {
	uint32_t key;
	uint32_t val;
} bucket_t;

typedef struct {
	pthread_t id;
	int thread;
	int threads;
	size_t inner_tuples;
	size_t outer_tuples;
	const uint32_t *inner_keys;
	const uint32_t *inner_vals;
	const uint32_t *outer_keys;
	const uint32_t *outer_vals;
	const uint32_t *outer_aggr_keys;
	bucket_t *table;
	uint32_t *bitmaps_multi;
	size_t partitions;
	int8_t log_partitions;
	size_t groups;
	int8_t log_buckets;
	size_t buckets;
	uint64_t sum;
	uint32_t count;
} thread_info_t;

uint32_t trailing_zero_count(uint32_t num)
{
	uint32_t i = num;
	uint32_t count = 0;
	while (i != 0) {
		if ((i & 1) == 1)
			return count;
		else {
			count++;
			i = i >> 1;
		}
	}
	return count;

}

int8_t log_two(size_t input)
{
	int8_t result = 0;
	size_t x = input;
	while (x > 0) {
		if (x == 1)
			break;
		else {
			result++;
			x /= 2;
		}
	}
	return result;
}

/*
 * helper function to atomically update global hash table
 * given aggregation key and the change in count and sum;
 *
 */
void update_global_table(uint32_t global_aggr_key,
			 uint32_t count_delta,
			 uint64_t sum_delta)
{
	uint32_t h_glb = (uint32_t) (global_aggr_key * BIG_NUMBER);
	h_glb >>= 32 - log_global_buckets;

	/*the key is likely to be in the table already*/
	if (global_table[h_glb].aggr_key == global_aggr_key)
		goto increment_bucket;

	/*atomically set bucket key*/
	while (!__sync_bool_compare_and_swap(
					     &global_table[h_glb].aggr_key,
					     0,
					     global_aggr_key)) {
		/* Check if compare and swap failed because the same key was
		 * just inserted by another thread; 
		 * Avoid duplicate key insertion
		 */
		if (global_table[h_glb].aggr_key == global_aggr_key)
			goto increment_bucket;

		h_glb = (h_glb + 1) & (global_buckets - 1);
	}
increment_bucket:
	__sync_fetch_and_add
		(&global_table[h_glb].count, count_delta);
	__sync_fetch_and_add
		(&global_table[h_glb].sum, sum_delta);
}

void *worker_thread(void *arg)
{
	thread_info_t *info = (thread_info_t *)arg;
	assert(pthread_equal(pthread_self(), info->id));

	/*copy info*/
	size_t thread = info->thread;
	size_t threads = info->threads;
	size_t inner_tuples = info->inner_tuples;
	const uint32_t *inner_keys = info->inner_keys;
	const uint32_t *inner_vals = info->inner_vals;
	bucket_t *table = info->table;
	int8_t log_buckets = info->log_buckets;
	size_t buckets = info->buckets;
	size_t outer_tuples = info->outer_tuples;
	const uint32_t *outer_keys = info->outer_keys;
	const uint32_t *outer_vals = info->outer_vals;
	const uint32_t *outer_aggr_keys = info->outer_aggr_keys;
	size_t partitions = info->partitions;
	const int8_t log_partitions = info->log_partitions;

	/*thread boundaries for inner table*/
	size_t inner_beg = (inner_tuples / threads) * (thread + 0);
	size_t inner_end = (inner_tuples / threads) * (thread + 1);
	if (thread + 1 == threads)
		inner_end = inner_tuples;

	/*hash inner tuples*/
	size_t i, h;
	for (i = inner_beg; i != inner_end; ++i) {
		uint32_t key = inner_keys[i];
		uint32_t val = inner_vals[i];
		h = (uint32_t) (key * BIG_NUMBER);
		h >>= 32 - log_buckets;
		while (!__sync_bool_compare_and_swap(&table[h].key, 0, key))
			h = (h + 1) & (buckets - 1);
		table[h].val = val;
	}

	/*estimate unique groups*/
	pthread_barrier_wait(&inner_table_barrier);
	size_t outer_beg = (outer_tuples / threads) * (thread + 0);
	size_t outer_end = (outer_tuples / threads) * (thread + 1);
	if (thread + 1 == threads)
		outer_end = outer_tuples;

	size_t j;

	/*create a local copy of the thread's own bitmap*/
	uint32_t *bitmaps_multi_local = calloc(partitions, 4);
	for (j = outer_beg; j != outer_end; ++j) {
		uint32_t h = (uint32_t)(outer_aggr_keys[j] * BIG_NUMBER);
		size_t p = h & (partitions - 1);
		h >>= log_partitions;
		bitmaps_multi_local[p] |= h & (-h);
	}
	/*copy the local copy to the bitmap packed in thread->info*/
	int bitmaps_multi_beg = partitions * thread;
	int bitmaps_multi_end = partitions * (thread + 1);
	for (j = bitmaps_multi_beg; j != bitmaps_multi_end; ++j)
		info->bitmaps_multi[j] = bitmaps_multi_local[j % partitions];

	free(bitmaps_multi_local);

	/*wait until all threads finish calculating bitmap*/
	pthread_barrier_wait(&global_hash_barrier);

	/*let thread 0 merge bitmaps and estimate groups*/
	if (thread == 0) {
		for (i = 0; i < partitions; ++i) {
			for (j = 1; j < threads; ++j) {
				info->bitmaps_multi[i] |=
					info->bitmaps_multi[i + j * partitions];
			}
		}

		int estimation = 0;
		for (i = 0; i < partitions; ++i)
			estimation += ((size_t) 1)
				<< trailing_zero_count(~info->bitmaps_multi[i]);

		/*round estimation to the nearest 2^k*/
		estimation /= 0.77351;
		global_buckets = estimation / 0.67;
		log_global_buckets = log_two(global_buckets) + 1;
		global_buckets = 1 << log_global_buckets;
		global_table = (aggr_bucket_t *)
			calloc(global_buckets, sizeof(aggr_bucket_t));

		for (i = 0; i < global_buckets; ++i) {
			global_table[i].aggr_key = 0;
			global_table[i].sum = 0;
			global_table[i].count = 0;
		}
	}

	/*TODO: come up with a policy;
	 * maybe compare global buckets and the number of outer tuples?
	 */
	aggr_bucket_t *local_table = NULL;

	if (LOCAL_CACHE_ENABLED) {
		/* create local hash table;
		 * L1 cache = 32K; each bucket = 16 Byte
		 * as for now, assign 2^10 buckets for local cache;
		 */
		local_table = (aggr_bucket_t *)
			calloc(local_buckets,  sizeof(aggr_bucket_t));
		assert(local_table != NULL);
		int i;
		for (i = 0; i < local_buckets; ++i) {
			local_table[i].aggr_key = 0;
			local_table[i].sum = 0;
			local_table[i].count = 0;
		}
	}

	/*join and aggregate*/
	pthread_barrier_wait(&global_table_creation);
	size_t o = 0;
	uint32_t count = 0;
	uint64_t sum = 0;
	for (o = outer_beg; o != outer_end; ++o) {
		uint32_t key = outer_keys[o];
		h = (uint32_t) (key * BIG_NUMBER);
		h >>= 32 - log_buckets;

		uint32_t tab = table[h].key;
		while (tab != 0) {
			if (tab == key) {
				uint64_t extra =
					table[h].val * (uint64_t)outer_vals[o];
				uint32_t aggr_key = outer_aggr_keys[o];

				/*check if local cache is enabled*/
				if (!LOCAL_CACHE_ENABLED) {
					update_global_table(aggr_key, 1, extra);
					break;
				}

				/*insert key to local hash table*/
				uint32_t h_local =
					(uint32_t)(aggr_key * BIG_NUMBER);
				h_local >>= 32 - log_local_buckets;
				if (local_table[h_local].aggr_key == aggr_key) {
					local_table[h_local].count++;
					local_table[h_local].sum += extra;
					break;
				}

				/* flush content in the bucket to global hash
				 * table if bucket is full*/
				if (local_table[h_local].aggr_key != 0)
					update_global_table(
							    local_table[h_local].aggr_key,
							    local_table[h_local].count,
							    local_table[h_local].sum);

				local_table[h_local].aggr_key = aggr_key;
				local_table[h_local].count = 1;
				local_table[h_local].sum = extra;
				break;
				}
			h = (h + 1) & (buckets - 1);
			tab = table[h].key;
		}
	}

	/* flush all local buckets to global hash table*/
	if (LOCAL_CACHE_ENABLED) {
		for (i = 0; i < local_buckets; ++i) {
			if (local_table[i].aggr_key != 0)
				update_global_table(
						    local_table[i].aggr_key,
						    local_table[i].count,
						    local_table[i].sum);
		}
		free(local_table);
	}


	pthread_barrier_wait(&aggr_barrier);
	size_t aggr_beg = (global_buckets / threads) * (thread + 0);
	size_t aggr_end = (global_buckets / threads) * (thread + 1);
	if (thread + 1 == threads)
		aggr_end = global_buckets;

	for (j = aggr_beg; j != aggr_end; ++j) {
		if ((global_table[j].count > 0
		     && global_table[j].aggr_key) != 0) {
			sum += global_table[j].sum / global_table[j].count;
			count++;
	    }
	}

	info->sum = sum;
	info->count = count;

	pthread_exit(NULL);
}

uint64_t q4112_run(const uint32_t *inner_keys, const uint32_t *inner_vals,
		   size_t inner_tuples, const uint32_t *outer_join_keys,
		   const uint32_t *outer_aggr_keys, const uint32_t *outer_vals,
		   size_t outer_tuples, int threads)
{
	int t, max_threads = sysconf(_SC_NPROCESSORS_ONLN);
	assert(max_threads > 0 && threads > 0 && threads <= max_threads);

	/*allocate space for the hash table*/
	int8_t log_buckets = 1;
	size_t buckets = 2;
	while (buckets * 0.67 < inner_tuples) {
		log_buckets += 1;
		buckets += buckets;
	}
	bucket_t *table = (bucket_t *) calloc(buckets, sizeof(bucket_t));
	assert(table != NULL);


	/* allocate bitmaps multiple version;*/
	const int8_t log_partitions = 12;
	size_t partitions = 1 << log_partitions;
	uint32_t *bitmaps_multi = (uint32_t *)calloc(threads * partitions, 4);
	assert(bitmaps_multi != NULL);

	pthread_barrier_init(&inner_table_barrier, NULL, threads);
	pthread_barrier_init(&global_hash_barrier, NULL, threads);
	pthread_barrier_init(&global_table_creation, NULL, threads);
	pthread_barrier_init(&aggr_barrier, NULL, threads);

	/*create worker threads;*/
	thread_info_t *info = (thread_info_t *)
		malloc(threads * sizeof(thread_info_t));
	assert(info != NULL);
	for (t = 0; t != threads; ++t) {
		info[t].outer_aggr_keys = outer_aggr_keys;
		info[t].bitmaps_multi = bitmaps_multi;
		info[t].partitions = partitions;
		info[t].log_partitions = log_partitions;
		info[t].groups = 0;
		info[t].thread = t;
		info[t].threads = threads;
		info[t].inner_tuples = inner_tuples;
		info[t].inner_keys = inner_keys;
		info[t].inner_vals = inner_vals;
		info[t].table = table;
		info[t].log_buckets = log_buckets;
		info[t].buckets = buckets;
		info[t].threads = threads;
		info[t].outer_tuples = outer_tuples;
		info[t].outer_keys = outer_join_keys;
		info[t].outer_vals = outer_vals;

		pthread_create(&info[t].id, NULL, worker_thread, &info[t]);

	}
	uint64_t sum = 0;
	uint32_t count = 0;
	/*aggregate result*/
	for (t = 0; t != threads; ++t) {
		pthread_join(info[t].id, NULL);
		sum += info[t].sum;
		count += info[t].count;
	}
	free(global_table);
	free(info);
	free(table);
	free(bitmaps_multi);
	return sum / count;

}
