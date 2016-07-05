/*
 * Copyright 2016, Intel Corporation
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in
 *       the documentation and/or other materials provided with the
 *       distribution.
 *
 *     * Neither the name of the copyright holder nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/*
 * replica.h -- module for synchronizing and transforming poolset
 */
#include "libpmempool.h"
#include "pool.h"

#define ALLOC_TAB_SIZE 4
#define PAGE_ALIGNED_SIZE(size) (size & ~(Pagesize - 1))

#define PREV_REP_PART_NO(cpart, nparts) ((nparts + cpart - 1) % nparts)
#define NEXT_REP_PART_NO(cpart, nparts) ((nparts + cpart + 1) % nparts)
#define NEAR_REPL(r, nrepl) ((unsigned)((nrepl + (r)) % (nrepl)))

/*
 * Keeps the table of already allocated replicas
 */
struct replica_alloc {
	/* keeps numbers of allocated replicas */
	unsigned repltab[ALLOC_TAB_SIZE];
	unsigned count;	/* number of already allocated replicas */
};

void add_alloc_replica(struct replica_alloc *alocrep, unsigned replno);
void close_replicas(struct replica_alloc *alocrep, struct pool_set *setin);

char *concatenate_str(const char *s1, const char *s2);
void grant_part_perm(struct pool_replica *repl, unsigned pstart, unsigned pend);
size_t get_part_data_len(struct pool_set *set_in, unsigned repl, unsigned part);
size_t get_part_range_data_len(struct pool_set *set_in, unsigned repl,
		unsigned pstart, unsigned pend);
uint64_t get_part_data_offset(struct pool_set *set_in, unsigned repl,
		unsigned part);

int map_parts_data(struct pool_set *set, unsigned repl, unsigned part_start,
		unsigned part_end, size_t data_len);
int remove_parts(struct pool_set *set_in, unsigned repl, unsigned pstart,
		unsigned pend);
int rename_parts(struct pool_set *set, unsigned repl, unsigned pstart,
		unsigned pend, const char *suffix);
bool is_dry_run(unsigned flags);
bool is_keep_orig(unsigned flags);

int sync_replica(struct pool_set *set_in, struct pmempool_replica_opts *opts);
int transform_replica(struct pool_set *set_in, struct pool_set *set_out,
		const unsigned flags);
