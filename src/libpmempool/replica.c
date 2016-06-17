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
 * replica.c -- groups all command for replica manipulation
 */

#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>

#include "out.h"
#include "libpmemobj.h"
#include "lane.h"
#include "redo.h"
#include "memops.h"
#include "pmalloc.h"
#include "list.h"
#include "set.h"
#include "file.h"
#include "mmap.h"

#include "replica.h"
/*
 * concatenate_str -- concatenate two strings
 */
char *
concatenate_str(const char *s1, const char *s2)
{
	char *result = malloc(strlen(s1) + strlen(s2) + 1);
	if (!result)
		return NULL;

	strcpy(result, s1);
	strcat(result, s2);

	return result;
}

/*
 * grant_part_perm -- set RW permission rights to the part from range
 */
void
grant_part_perm(struct pool_replica *repl, unsigned pstart, unsigned pend)
{
	for (unsigned p = pstart; p < pend; p++) {
		chmod(repl->part[p].path, S_IRUSR | S_IWUSR);
	}
}

/*
 * get_part_data_len -- get data length for given part
 */
size_t
get_part_data_len(struct pool_set *set_in, unsigned repl, unsigned part)
{
	size_t len;
	len = PAGE_ALIGNED_SIZE(set_in->replica[repl]->part[part].filesize) -
			POOL_HDR_SIZE;
	return len;
}

/*
 * get_part_range_data_len -- get data length in given range
 */
size_t
get_part_range_data_len(struct pool_set *set_in, unsigned repl,
		unsigned pstart, unsigned pend)
{
	size_t len = 0;
	for (unsigned i = pstart; i < pend; ++i) {
		len += get_part_data_len(set_in, repl, i);
	}

	return len;
}

/*
 * get_part_data_offset -- get data length before given part
 */
uint64_t
get_part_data_offset(struct pool_set *set_in, unsigned repl, unsigned part)
{
	uint64_t pdoff = 0;
	for (unsigned i = 0; i < part; ++i)
		pdoff += get_part_data_len(set_in, repl, i);

	return pdoff;
}

/*
 * map_parts_data -- map parts from given range
 */
int
map_parts_data(struct pool_set *set, unsigned repl, unsigned part_start,
		unsigned part_end, size_t data_len)
{
	struct pool_replica *rep = set->replica[repl];

	/* determine a hint address for mmap() */
	void *addr = util_map_hint(data_len, 0);
	if (addr == MAP_FAILED) {
		ERR("cannot find a contiguous region of given size");
		return -1;
	}

	/*
	 * map the first part we copy from and reserve space for
	 * remaining parts
	 */
	size_t mapfrom_size = get_part_range_data_len(set, repl,
			part_start, part_end);
	if (util_map_part(&rep->part[part_start],
			addr, mapfrom_size, POOL_HDR_SIZE,
			MAP_SHARED) != 0) {
		ERR("pool mapping failed");
		return -1;
	}

	size_t mapsize = PAGE_ALIGNED_SIZE(
		rep->part[part_start].filesize) - POOL_HDR_SIZE;
	addr = (char *)rep->part[part_start].addr + mapsize;

	/* map the remaining parts of the usable pool space */
	for (unsigned i = part_start + 1;
			i < part_end; ++i) {
		if (util_map_part(&rep->part[i], addr, 0, POOL_HDR_SIZE,
				MAP_SHARED | MAP_FIXED) != 0) {
			ERR("usable space mapping failed - part #%d", i);
			goto err;
		}

		mapsize += rep->part[i].size;
		addr = (char *)addr + rep->part[i].size;
	}

	ASSERTeq(mapsize, mapfrom_size);
	return 0;
err:
	util_unmap_part(&rep->part[part_start]);
	return -1;
}

/*
 * remove_parts -- unlink parts from damaged replica
 */
int
remove_parts(struct pool_set *set_in, unsigned repl, unsigned pstart,
		unsigned pend)
{
	struct pool_replica *replica = set_in->replica[repl];

	for (unsigned i = pstart; i < pend; ++i) {
		if (unlink(replica->part[i].path)) {
			if (errno != ENOENT)
				return -1;
		}
	}
	return 0;
}

/*
 * rename_parts -- rename parts by adding suffix
 */
int
rename_parts(struct pool_set *set, unsigned repl, unsigned pstart,
		unsigned pend, const char *suffix)
{
	int result = 0;
	char *path_suffix = NULL;
	if (!suffix)
		return -1;

	for (unsigned i = pstart; i < pend; ++i) {
		struct pool_set_part *part = &set->replica[repl]->part[i];

		path_suffix = concatenate_str(part->path, suffix);
		if (!path_suffix)
			return -1;

		if (rename(part->path, path_suffix))
			result = -1;
	}

	if (path_suffix)
		free(path_suffix);
	return result;
}

/*
 * is_replica_alloc -- check if replica is on list of allocated replicas
 */
static bool
is_replica_alloc(struct replica_alloc *alocrep, unsigned replno)
{
	for (unsigned i = 0; i < alocrep->count; ++i) {
		if (alocrep->repltab[i] == replno)
			return true;
	}
	return false;
}

/*
 * add_alloc_replica -- add to list of allocated replicas
 */
void
add_alloc_replica(struct replica_alloc *alocrep, unsigned replno)
{
	if (is_replica_alloc(alocrep, replno))
		return;

	alocrep->repltab[alocrep->count++] = replno;
}

/*
 * close_replicas -- close all opened replicas
 */
void
close_replicas(struct replica_alloc *alocrep, struct pool_set *setin)
{
	for (unsigned i = 0; i < alocrep->count; ++i) {
		unsigned replno = alocrep->repltab[i];
		util_replica_close_part(setin, replno);
		struct pool_replica *repl = setin->replica[replno];
		util_replica_fdclose(repl);
	}
}

/*
 * is_dry_run -- check whether only verification mode is enabled
 */
inline bool
is_dry_run(unsigned flags)
{
	return PMEMPOOL_REPLICA_VERIFY & flags;
}

/*
 * is_keep_orig -- check whether keep original files
 */
inline bool
is_keep_orig(unsigned flags)
{
	return PMEMPOOL_REPLICA_KEEP_ORIG & flags;
}

/*
 * pmempool_sync -- copy one replica to another
 */
int
pmempool_sync(const char *poolset, struct pmempool_replica_opts *opts)
{
	int result;

	if (poolset == NULL) {
		ERR("poolset paths can not be NULL");
		errno = EINVAL;
		return -1;
	}

	/* check if poolset has correct signature */
	if (util_is_poolset(poolset) != 1) {
		ERR("!util_is_poolset");
		result = -1;
		goto err;
	}

	/* open poolset file */
	int fd_in = util_file_open(poolset, NULL, 0, O_RDONLY);
	if (fd_in < 0) {
		ERR("!util_file_open");
		result = -1;
		goto err;
	}

	/* fill up pool_set structure */
	struct pool_set *set_in = NULL;
	if (util_poolset_parse(poolset, fd_in, &set_in)) {
		ERR("Parsing input poolset failed");
		result = -1;
		goto err_close;
	}

	/* copy data from one replica to another */
	result = sync_replica(set_in, opts);

err_close:
	util_poolset_free(set_in);
	close(fd_in);
err:
	if (result != 0 && errno == 0)
		errno = EINVAL;
	return result;
}

/*
 * pmempool_poolset_convert -- alter poolset structure
 */
int
pmempool_transform(const char *poolset_in,
		const char *poolset_out, unsigned flags)
{
	int result;

	if (poolset_in == NULL || poolset_out == NULL) {
		ERR("poolset paths can not be NULL");
		errno = EINVAL;
		return -1;
	}

	if (util_is_poolset(poolset_in) != 1) {
		ERR("!util_is_poolset - input path");
		result = -1;
		goto err;
	}

	if (util_is_poolset(poolset_out) != 1) {
		ERR("!util_is_poolset - output path");
		result = -1;
		goto err;
	}

	int fd_in = util_file_open(poolset_in, NULL, 0, O_RDONLY);
	if (fd_in < 0) {
		ERR("!util_file_open - input path");
		result = -1;
		goto err;
	}

	int fd_out = util_file_open(poolset_out, NULL, 0, O_RDONLY);
	if (fd_out < 0) {
		ERR("!util_file_open - output path");
		result = -1;
		goto err_close_fin;
	}

	struct pool_set *set_in = NULL;
	struct pool_set *set_out = NULL;

	/* parse input poolset file */
	if (util_poolset_parse(poolset_in, fd_in, &set_in)) {
		ERR("!util_poolset_parse - input path");
		result = -1;
		goto err_close_finout;
	}

	/* parse output poolset file */
	if (util_poolset_parse(poolset_out, fd_out, &set_out)) {
		ERR("!util_poolset_parse - output path");
		result = -1;
		goto err_close_poolinfree;
	}

	/* transform poolset */
	result = transform_replica(set_in, set_out, flags);

	util_poolset_free(set_out);
err_close_poolinfree:
	util_poolset_free(set_in);
err_close_finout:
	close(fd_out);
err_close_fin:
	close(fd_in);
err:
	if (result != 0 && errno == 0)
		errno = EINVAL;
	return result;
}
