#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

typedef struct {
	uint32_t key;
	uint32_t val;
} bucket_t;

typedef struct {
	pthread_t id;
	int thread;
	int threads;
	size_t outer_tuples;
	const uint32_t* outer_keys;
	const uint32_t* outer_vals;
	uint64_t sum;
	uint32_t count;
	bucket_t* table;
	int8_t log_buckets;
	size_t buckets;
} q4112_run_info_t;

typedef struct {
	pthread_t id;
	int thread;
	int threads;
	size_t inner_tuples;
	const uint32_t* inner_keys;
	const uint32_t* inner_vals;
	bucket_t* table;
	int8_t log_buckets;
	size_t buckets;
} table_builder_info_t;

void* table_builder_thread(void *arg) {
	table_builder_info_t* info = (table_builder_info*)arg;
	assert(pthread_equal(pthread_self(), info->id));

	//copy info
	size_t thread = info->thread;
	size_t threads = info->threads;
	size_t inner_tuples = info->inner_tuples;
	const uint32_t* inner_keys = info->inner_keys;
	const uint32_t* inner_vals = info->inner_vals;
	bucket_t* table = info->table;
	int8_t log_buckets = info->log_buckets;
	size_t buckets = info->buckets;

	//thread boundaries for inner table
	size_t inner_beg = (inner_tuples / threads) * (thread + 0);
	size_t inner_end = (inner_tuples / threads) * (thread + 1);
	if (thread + 1 == threads)
		inner_end = inner_tuples;

	//put inner tuples into the table
	size_t i, h;
	for (i = inner_beg; i != inner_end; ++i) {
		uint32_t key = inner_keys[i];
		uint32_t val = inner_vals[i];
		//calculate hash value
		h = (uint32_t) (key * 0x9e3779b1);
		h >>= 32 - log_buckets;
		//TODO: figure out the type of table[h]
		while (!__sync_bool_compare_and_swap(&table[h].key, 0, key)) {
			h = (h + 1) & (buckets - 1);
		}
		table[h].val = val;
	}
	pthread_exit(NULL);
}
void* q4112_run_thread(void *arg) {
	q4112_run_info_t* info = (q4112_run_info_t*) arg;
	assert(pthread_equal(pthread_self(), info->id));
	//copy info
	size_t thread = info->thread;
	size_t threads = info->threads;
	size_t outer_tuples = info->outer_tuples;
	const uint32_t* outer_keys = info->outer_keys;
	const uint32_t* outer_vals = info->outer_vals;
	bucket_t* table = info->table;
	int8_t log_buckets = info->log_buckets;
	size_t buckets = info->buckets;
	//thread boundaries for outer table
	size_t outer_beg = (outer_tuples / threads) * (thread + 0);
	size_t outer_end = (outer_tuples / threads) * (thread + 1);
	if (thread + 1 == threads)
		outer_end = outer_tuples;

	//do hash join here
	size_t i, o, h;
	uint32_t count = 0;
	uint64_t sum = 0;
	for (o = outer_beg; o != outer_end; ++o) {
		uint32_t key = outer_keys[o];
		h = (uint32_t) (key * 0x9e3779b1);
		h >>= 32 - log_buckets;

		uint32_t tab = table[h].key;
		while (tab != 0) {
			if (tab == key) {
				sum += table[h].val * (uint64_t)outer_vals[o];
				count += 1;
				break;
			}
			h = (h + 1) & (buckets - 1);
			tab = table[h].key;
		}
	}
	info->sum = sum;
	info->count = count;
	pthread_exit(NULL);
}

uint64_t q4112_run(const uint32_t* inner_keys, const uint32_t* inner_vals,
		   size_t inner_tuples, const uint32_t* outer_join_keys,
		   const uint32_t* outer_aggr_keys, const uint32_t* outer_vals,
		   size_t outer_tuples, int threads) {
	int t, max_threads = sysconf(_SC_NPROCESSORS_ONLN);
	assert(max_threads > 0 && threads > 0 && threads <= max_threads);
	//TODO: allocate space for the hash table
	int8_t log_buckets = 1;
	size_t buckets = 2;
	while (buckets * 0.67 < inner_tuples) {
		log_buckets += 1;
		buckets += buckets;
	}
	bucket_t* table = (bucket_t*) calloc(buckets, sizeof(bucket_t));
	assert(table != NULL);
	//TODO: create some threads;
	table_builder_info_t* builder_info = (table_builder_info_t*)
		malloc(threads * sizeof(table_builder_info_t));
	assert(info != NULL);
	for (t = 0; t != threads; ++t) {
		builder_info[t].thread = t;
		builder_info[t].threads = threads;
		builder_info[t].inner_tuples = inner_tuples;
		builder_info[t].inner_keys = inner_keys;
		builder_info[t].inner_vals = inner_vals;
		builder_info[t].table = table;
		builder_info[t].log_buckets = log_buckets;
		builder_info[t].buckets = buckets;
		pthread_create(&builder_info[t].id, NULL, table_builder_thread, 
			       &builder_info[t]);
	}
	for (t = 0; t != threads; ++t) {
		pthread_join(builder_info[t].id, NULL);
	}
	free(builder_info);
	return 1;
	//TODO: fill in the hash table with multiple threads
	//TODO: join using multiple threads
	//gather results	

}
