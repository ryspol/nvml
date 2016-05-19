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
 * replica_sync.c -- module for synchronize spoolset
 */

#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>

#include "out.h"
#include "libpmemobj.h"
#include "lane.h"
#include "redo.h"
#include "memops.h"
#include "pmalloc.h"
#include "list.h"
#include "libpmempool.h"
#include "replica_sync.h"
#include "obj.h"

enum repl_uuid_dir
{
	REPLICA_UUIDDIR_PREV,
	REPLICA_UUIDDIR_NEXT
};

#define ADDR_SUM(vp, lp) ((void *)((char *)(vp) + lp))


/*
 * part_fdclose -- close all parts of given replica
 */
static void
part_fdclose(struct pool_replica *rep)
{
	for (unsigned p = 0; p < rep->nparts; p++) {
		struct pool_set_part *part = &rep->part[p];

		if (part->fd != -1)
			close(part->fd);
	}
}

/*
 * create_parts -- create files for given replica
 */
static int
create_parts(struct pool_set *set_in, unsigned repl)
{
	struct pool_replica *replica = set_in->replica[repl];
	for (unsigned i = 0; i < replica->nparts; ++i) {
		if (util_poolset_file(&replica->part[i], 0, 1))
			return -1;
	}
	return 0;
}

/*
 * remove_parts -- unlink all parts of replica
 */
static int
remove_parts(struct pool_set *set_in, unsigned repl)
{
	struct pool_replica *replica = set_in->replica[repl];

	for (unsigned i = 0; i < replica->nparts; ++i) {
		if (unlink(replica->part[i].path)) {
			if (errno != ENOENT)
				return -1;
		}
	}
	return 0;
}

/*
 * open_files_mmap_headers -- open parts and map headers
 */
static int
open_files_mmap_headers(struct pool_replica *repl)
{
	/* open all parts */
	for (unsigned p = 0; p < repl->nparts; p++) {
		if (util_poolset_file(&repl->part[p], 0, 0)) {
			LOG(2, "Cannot open file - part #%d", p);
			return -1;
		}
	}

	/* map all headers - don't care about the address */
	for (unsigned p = 0; p < repl->nparts; p++) {
		if (util_map_hdr(&repl->part[p], MAP_SHARED) != 0) {
			LOG(2, "Header mapping failed - part #%d", p);
			return -1;
		}
	}
	return 0;
}

/*
 * update_adjacent_uuid -- set next or previous uuid in header
 */
static int
update_adjacent_uuid(struct pool_replica *rep, enum repl_uuid_dir uid_dir,
		uuid_t *uuid)
{
	for (unsigned p = 0; p < rep->nparts; p++) {
		struct pool_hdr *hdrp = rep->part[p].hdr;
		if (uid_dir == REPLICA_UUIDDIR_PREV) {
			memcpy(hdrp->prev_repl_uuid, uuid, POOL_HDR_UUID_LEN);
		} else {
			memcpy(hdrp->next_repl_uuid, uuid, POOL_HDR_UUID_LEN);
		}
		util_checksum(hdrp, sizeof(*hdrp), &hdrp->checksum, 1);
	}

	return 0;
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
static void
add_alloc_replica(struct replica_alloc *alocrep, unsigned replno)
{
	if (is_replica_alloc(alocrep, replno))
		return;

	alocrep->repltab[alocrep->count++] = replno;
}

/*
 * close_replicas -- close all opened replicas
 */
static void
close_replicas(struct replica_alloc *alocrep, struct pool_set *setin)
{
	for (unsigned i = 0; i < alocrep->count; ++i) {
		unsigned replno = alocrep->repltab[i];
		util_replica_close(setin, replno);
		struct pool_replica *repl = setin->replica[replno];
		part_fdclose(repl);
	}
}


/*
 * update_uuids -- set next and previous uuids in pool_set structure
 */
static int
update_uuids(struct pool_set *set_in, unsigned replto, unsigned replfrom,
		struct replica_alloc *alocrep)
{
	struct pool_replica *replica = set_in->replica[replto];
	unsigned repl_no = (set_in)->nreplicas;

	unsigned nprev = (repl_no + replto - 1) % repl_no;
	unsigned nnext = (repl_no + replto + 1) % repl_no;

	struct pool_replica *rprev = set_in->replica[nprev];
	struct pool_replica *rnext = set_in->replica[nnext];

	if (!is_replica_alloc(alocrep, nprev)) {
		if (open_files_mmap_headers(rprev))
			return -1;
		else
			add_alloc_replica(alocrep, nprev);
	}

	if (!is_replica_alloc(alocrep, nnext)) {
		if (open_files_mmap_headers(rnext))
			return -1;
		else
			add_alloc_replica(alocrep, nnext);
	}

	uuid_t *curr_uuid = &replica->part[0].uuid;

	update_adjacent_uuid(rprev, REPLICA_UUIDDIR_NEXT, curr_uuid);
	update_adjacent_uuid(rnext, REPLICA_UUIDDIR_PREV, curr_uuid);

	return 0;
}

/*
 * fill_struct_adjacent_uuids -- set uuids in pool_set structure
 */
static int
fill_struct_adjacent_uuids(struct pool_set *set_in, unsigned repl)
{
	struct pool_set_part *rnext_part0 = &PART(REP(set_in, repl + 1), 0);
	struct pool_set_part *rprev_part0 = &PART(REP(set_in, repl - 1), 0);

	struct pool_hdr *hdrp = rnext_part0->hdr;
	memcpy(rnext_part0->uuid, hdrp->uuid, POOL_HDR_UUID_LEN);

	hdrp = rprev_part0->hdr;
	memcpy(rprev_part0->uuid, hdrp->uuid, POOL_HDR_UUID_LEN);
	return 0;
}

/*
 * grant_part_perm -- set RWX permission rights to the part
 */
static void
grant_part_perm(struct pool_replica *repl)
{
	for (unsigned p = 0; p < repl->nparts; p++) {
		chmod(repl->part[p].path, S_IRWXU | S_IRWXG | S_IRWXO);
	}
}

/*
 * sync_replica -- copy one replica into another
 */
enum pmempool_replica_result
sync_replica(struct pool_set *set_in, unsigned repl_to,
		unsigned repl_from, uint32_t flags)
{
	enum pmempool_replica_result result = REPLICA_RES_COPY_SUCCESSFUL;
	struct replica_alloc alloc_rep = {.count = 0};

	struct pool_replica *rto = set_in->replica[repl_to];
	struct pool_replica *rfrom = set_in->replica[repl_from];

	/* check replica numbers */
	if (repl_to >= set_in->nreplicas || repl_from >= set_in->nreplicas) {
		LOG(2, "No such replica number in poolset");
		result = REPLICA_RES_WRONG_REPL_NUM;
		goto error;
	}

	/* remove all parts from damaged replica */
	if (remove_parts(set_in, repl_to)) {
		result = REPLICA_RES_PART_FILE_DEL_ERR;
		LOG(2, "Cannot remove parts");
		goto error;
	}

	/* create new parts for given replica */
	if (create_parts(set_in, repl_to)) {
		result = REPLICA_RES_PART_FILE_CREATE_ERR;
		LOG(2, "Cannot remove parts");
		goto error;
	}

	/* generate new uuids for all parts of newly created replica */
	for (unsigned i = 0; i < rto->nparts; i++) {
		if (util_uuid_generate(rto->part[i].uuid) < 0) {
			LOG(2, "Cannot generate pool set part UUID");
			result = REPLICA_RES_INTERNAL_ERR;
			goto error;
		}
	}

	/* open all parts of replica we copy from */
	for (unsigned p = 0; p < rfrom->nparts; p++) {
		if (util_poolset_file(&rfrom->part[p], 0, 0)) {
			LOG(2, "Cannot open file - part #%d", p);
			return -1;
		}
	}

	/* open and map whole replica we copy from */
	if (util_replica_open(set_in, repl_from, MAP_SHARED) == 0) {
		add_alloc_replica(&alloc_rep, repl_from);
	} else {
		LOG(2, "Replica open failed");
		result = REPLICA_RES_REP_OPEN_ERR;
		goto error;
	}

	/* fill up uuids of next and prev replica referenced to newly created */
	fill_struct_adjacent_uuids(set_in, repl_to);

	/* fill up poolset uuid */
	struct pool_hdr *hdrp = set_in->replica[repl_from]->part[0].hdr;
	memcpy(set_in->uuid, hdrp->poolset_uuid, POOL_HDR_UUID_LEN);

	/* create new replica */
	if (util_replica_create(set_in, repl_to, MAP_SHARED, OBJ_HDR_SIG,
			OBJ_FORMAT_MAJOR, OBJ_FORMAT_COMPAT,
			OBJ_FORMAT_INCOMPAT, OBJ_FORMAT_RO_COMPAT,
			NULL, NULL) != 0) {
		LOG(2, "Replica creation failed");
		result = REPLICA_RES_REP_CREATE_ERR;
		goto error;
	} else {
		add_alloc_replica(&alloc_rep, repl_to);
	}

	/* check sizes */
	size_t copy_size = rfrom->repsize;
	if (rto->repsize < rfrom->repsize) {
		if (PMEMPOOL_REPLICA_TRUNCATE & flags) {
			copy_size = rto->repsize;
		} else {
			LOG(2, "Not enough memory to copy to target replica");
			result = REPLICA_RES_INSUF_TARGET_MEM;
			goto error;
		}
	}

	/* copy all data */
	memcpy(ADDR_SUM(rto->part[0].addr, POOL_HDR_SIZE),
			ADDR_SUM(rfrom->part[0].addr, POOL_HDR_SIZE),
			copy_size - POOL_HDR_SIZE);

	/* grand permission for all created parts */
	grant_part_perm(rto);

	/* update prev and next uuids of neighboring replicas */
	if (update_uuids(set_in, repl_to, repl_from, &alloc_rep)) {
		LOG(2, "Updating prev and next uids failed");
		result = REPLICA_RES_CANNOT_UUIDS_UPDATE;
	}

error:
	close_replicas(&alloc_rep, set_in);
	return result;
}
