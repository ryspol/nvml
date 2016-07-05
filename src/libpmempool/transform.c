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
 * transform.c -- module for pool set transforming
 */

#include <stdio.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>

#include "replica.h"
#include "out.h"
#include "libpmemobj.h"
#include "lane.h"
#include "redo.h"
#include "memops.h"
#include "pmalloc.h"
#include "list.h"
#include "libpmempool.h"
#include "obj.h"

#define TEMP_FILE_SUFFIX "_temp"
#define COPY_FILE_SUFFIX "_old"

/*
 * Value points which replica is active
 */
enum active_iteration_replica {
	ITERATION_REPL_IN, /* input replica is active */
	ITERATION_REPL_OUT /* output replica is active */
};

/*
 * Structure keeps the context of region searching mechanism
 */
struct region {
	unsigned replica;

	unsigned part_first_out; /* first region part from output replica */
	unsigned part_last_out;	/* last region part from output replica */

	unsigned part_first_in;	/* first region part from input replica */
	unsigned part_last_in;	/* last region part from input replica */

	size_t data_len;	/* number of bytes of data to copy */
};

/*
 * Structure keeps the context transformation
 */
struct transform_context {

	struct region *region_list; /* list of regions to process */
	unsigned region_no;	/* number of regions to process */
};

/*
 * Structure keeps the context of region searching mechanism
 */
struct part_search_context {
	/*
	 * sum of file sizes of previously processed files
	 * from active replica
	 */
	uint64_t active_indicator;
	/*
	 * sum of file sizes of previously processed files
	 * from checking replica
	 */
	uint64_t check_indicator;

	unsigned active_cnt; /* part counter of active replica */
	unsigned check_cnt; /* part counter of checking replica */

	struct pool_replica *active_replica; /* active replica */
	struct pool_replica *check_replica; /* checking replica */

	/* points which replica is active - through which iterate iter_cnt */
	enum active_iteration_replica activ_repl;
};

/*
 * verify_arguments -- check correctness of passed arguments
 */
static int
verify_arguments(struct pool_set *set_in, struct pool_set *set_out,
		unsigned flags)
{
	if (is_dry_run(flags) && is_keep_orig(flags)) {
		ERR("Passed flags cannot be enable in the same time");
		errno = EINVAL;
		return -1;
	}

	if (set_in->nreplicas != set_out->nreplicas) {
		ERR("Different numbers of replicas in poolsets"
				"are not supported");
		errno = ENOSYS;
		return -1;
	}
	return 0;
}

/*
 * filepath_match -- check if corresponding files in input and output replica
 * has the same path
 */
static int
filepath_match(struct part_search_context *part_ctx)
{

	struct pool_set_part *piter =
			&part_ctx->active_replica->part[part_ctx->active_cnt];
	struct pool_set_part *pcheck =
			&part_ctx->check_replica->part[part_ctx->check_cnt];

	return strcmp(piter->path, pcheck->path) == 0;
}

/*
 * check_counters_overflow -- check if part counters in part context did not
 * exceed the number of parts in replica
 */
inline static int
check_counters_overflow(struct part_search_context *part_ctx)
{
	return part_ctx->active_cnt >= part_ctx->active_replica->nparts ||
		part_ctx->check_cnt >= part_ctx->check_replica->nparts;
}

/*
 * increase_part_ctx_indicators -- increase indicators from part context
 * by adding size gain of currently processed files
 */
inline static void
increase_part_ctx_indicators(struct part_search_context *part_ctx)
{
	part_ctx->active_indicator += PAGE_ALIGNED_SIZE(
		part_ctx->active_replica->part[part_ctx->active_cnt].filesize) -
		POOL_HDR_SIZE;
	part_ctx->check_indicator += PAGE_ALIGNED_SIZE(
		part_ctx->check_replica->part[part_ctx->check_cnt].filesize) -
		POOL_HDR_SIZE;
}

/*
 * set_region_first_parts -- set first input and output parts in region
 */
inline static void
set_region_first_parts(struct part_search_context *part_ctx,
		struct transform_context *trans_ctx)
{
	if (part_ctx->activ_repl == ITERATION_REPL_OUT) {
		trans_ctx->region_list[trans_ctx->region_no - 1]
			.part_first_out = part_ctx->active_cnt;
		trans_ctx->region_list[trans_ctx->region_no - 1]
			.part_first_in = part_ctx->check_cnt;
	} else {
		trans_ctx->region_list[trans_ctx->region_no - 1]
			.part_first_out = part_ctx->check_cnt;
		trans_ctx->region_list[trans_ctx->region_no - 1]
			.part_first_in = part_ctx->active_cnt;
	}
}

/*
 * process_equal_parts -- iterate through parts of replica that are
 * the same in terms of size and path. Different size or path points
 * the beginning of new region which is allocated in this function.
 */
static int
process_equal_parts(struct part_search_context *part_ctx,
		struct transform_context *trans_ctx)
{
	struct region *realloc_region;

	while (part_ctx->active_indicator == part_ctx->check_indicator &&
			filepath_match(part_ctx) == true) {

		part_ctx->active_cnt++;
		part_ctx->check_cnt++;
		if (check_counters_overflow(part_ctx)) {
			if (part_ctx->active_indicator ==
					part_ctx->check_indicator)
				return 0;
			else
				return -1;
		}

		increase_part_ctx_indicators(part_ctx);
	}

	trans_ctx->region_no++;
	realloc_region = realloc(trans_ctx->region_list,
		trans_ctx->region_no * sizeof(struct region));

	if (realloc_region) {
		trans_ctx->region_list = realloc_region;
		set_region_first_parts(part_ctx, trans_ctx);

	} else {
		return -1;
	}

	return 0;
}

/*
 * process_different_parts -- iterate through parts of replica that differ
 * in size or path. One or more such parts make up one region used in
 * transformation to new replica's shape.
 */
static int
process_different_parts(struct part_search_context *part_ctx,
		struct transform_context *trans_ctx)
{
	struct region *curr_region =
			&trans_ctx->region_list[trans_ctx->region_no - 1];

	curr_region->data_len = PAGE_ALIGNED_SIZE(
			part_ctx->active_replica->part[part_ctx->active_cnt]
				.filesize) - POOL_HDR_SIZE;

	while (part_ctx->active_indicator != part_ctx->check_indicator) {
		if (part_ctx->active_indicator < part_ctx->check_indicator) {
			part_ctx->active_cnt++;
			if (check_counters_overflow(part_ctx))
				return -1;

			size_t fsize = PAGE_ALIGNED_SIZE(
				part_ctx->active_replica->
					part[part_ctx->active_cnt].filesize);
			part_ctx->active_indicator += fsize - POOL_HDR_SIZE;
			curr_region->data_len += fsize - POOL_HDR_SIZE;
		} else {
			part_ctx->check_cnt++;
			if (check_counters_overflow(part_ctx))
				return -1;

			part_ctx->check_indicator +=
				PAGE_ALIGNED_SIZE(part_ctx->check_replica->
				part[part_ctx->check_cnt].filesize) -
				POOL_HDR_SIZE;
		}
	}
	curr_region->part_last_out = (part_ctx->activ_repl ==
			ITERATION_REPL_OUT) ? part_ctx->active_cnt :
			part_ctx->check_cnt;
	curr_region->part_last_in = (part_ctx->activ_repl ==
			ITERATION_REPL_OUT) ? part_ctx->check_cnt :
			part_ctx->active_cnt;

	return 0;
}

/*
 * find_regions -- find all regions to be transformed
 */
static int
find_regions(struct pool_set *set_in, struct pool_set *set_out,
		unsigned repl, struct transform_context *trans_ctx)
{
	struct part_search_context part_ctx = {
		.active_indicator = 0,
		.check_indicator = 0,
		.active_cnt = 0,
		.check_cnt = 0
	};

	if (set_in->replica[repl]->nparts > set_out->replica[repl]->nparts) {
		part_ctx.active_replica = set_in->replica[repl];
		part_ctx.check_replica = set_out->replica[repl];
		part_ctx.activ_repl = ITERATION_REPL_IN;
	} else {
		part_ctx.active_replica = set_out->replica[repl];
		part_ctx.check_replica = set_in->replica[repl];
		part_ctx.activ_repl = ITERATION_REPL_OUT;
	}

	part_ctx.active_indicator = PAGE_ALIGNED_SIZE(
		part_ctx.active_replica->part[0].filesize) - POOL_HDR_SIZE;
	part_ctx.check_indicator = PAGE_ALIGNED_SIZE(
		part_ctx.check_replica->part[0].filesize) - POOL_HDR_SIZE;

	while (part_ctx.active_cnt < part_ctx.active_replica->nparts) {
		if (process_equal_parts(&part_ctx, trans_ctx))
			return -1;

		if (check_counters_overflow(&part_ctx))
			return 0;

		trans_ctx->region_list[trans_ctx->region_no - 1].replica = repl;

		if (process_different_parts(&part_ctx, trans_ctx))
			return -1;

		part_ctx.active_cnt++;
		part_ctx.check_cnt++;
		if (check_counters_overflow(&part_ctx))
			return 0;

		increase_part_ctx_indicators(&part_ctx);
	}
	return 0;
}

/*
 * create_region_temp_files -- create output files for given region using
 * temporary names
 */
static int
create_region_temp_files(struct pool_set *set_out,
		struct region *reg)
{
	int result = 0;
	char *path_suffix = NULL;
	for (unsigned i = reg->part_first_out; i <= reg->part_last_out; ++i) {
		struct pool_set_part *part =
			&set_out->replica[reg->replica]->part[i];

		char *path_suffix = concatenate_str(part->path,
				TEMP_FILE_SUFFIX);
		if (!path_suffix)
			return -1;

		part->fd = util_file_create(path_suffix, part->filesize, 0);
		if (part->fd == -1) {
			LOG(2, "failed to create file: %s", path_suffix);
			result = -1;
			goto err;
		}
		part->created = 1;
	}

err:
	if (path_suffix)
		free(path_suffix);
	return result;
}

/*
 * create_poolset_temp_files -- create output files for all regions using
 * temporary names
 */
static int
create_poolset_temp_files(struct pool_set *set_out,
		struct transform_context *trans_ctx)
{
	for (unsigned i = 0; i < trans_ctx->region_no; ++i) {
		struct region *reg = &trans_ctx->region_list[i];
		if (create_region_temp_files(set_out, reg))
			return -1;
	}
	return 0;
}

/*
 * open_parts_input_region -- open input parts from all regions
 */
static int
open_parts_input_region(struct pool_set *set_in,
		struct transform_context *trans_ctx)
{
	for (unsigned i = 0; i < trans_ctx->region_no; ++i) {
		struct region *reg = &trans_ctx->region_list[i];
		for (unsigned i = reg->part_first_in;
			i <= reg->part_last_in; ++i) {
			if (util_poolset_file(&set_in->replica[reg->replica]->
				part[i], 0, 0))
					return -1;
		}
	}
	return 0;
}

/*
 * close_poolset_replicas -- close all opened replicas
 */
static void
close_poolset_replicas(struct pool_set *set_in, struct pool_set *set_out,
		struct transform_context *trans_ctx)
{
	struct replica_alloc alloc_rep = {.count = 0};

	for (unsigned i = 0; i < trans_ctx->region_no; ++i)
		add_alloc_replica(&alloc_rep,
				trans_ctx->region_list[i].replica);

	close_replicas(&alloc_rep, set_in);
	close_replicas(&alloc_rep, set_out);
}

/*
 * map_in_out_parts -- map all parts from input and output parts of regions
 */
static int
map_in_out_parts(struct pool_set *set_in, struct pool_set *set_out,
		struct transform_context *trans_ctx)
{
	for (unsigned i = 0; i < trans_ctx->region_no; ++i) {
		struct region *curr_reg = &trans_ctx->region_list[i];
		if (map_parts_data(set_out, curr_reg->replica,
				curr_reg->part_first_out,
				curr_reg->part_last_out + 1,
				curr_reg->data_len))
			goto error;

		if (map_parts_data(set_in, curr_reg->replica,
				curr_reg->part_first_in,
				curr_reg->part_last_in + 1,
				curr_reg->data_len))
			goto error;
	}
	return 0;

error:
	ERR("Cannot map input/output parts");
	errno = EINVAL;
	return -1;
}

/*
 * copy_data -- copy data from all regions
 */
static int
copy_data(struct pool_set *set_in, struct pool_set *set_out,
		struct transform_context *trans_ctx)
{
	for (unsigned i = 0; i < trans_ctx->region_no; ++i) {
		struct region *curr_reg = &trans_ctx->region_list[i];

		void *src = set_in->replica[curr_reg->replica]->
				part[curr_reg->part_first_in].addr;
		void *dst = set_out->replica[curr_reg->replica]->
				part[curr_reg->part_first_out].addr;
		memcpy(dst, src, curr_reg->data_len);
	}

	return 0;
}

/*
 * open_map_header -- open file and map header
 */
static int
open_map_header(struct pool_set_part *pin)
{
	if (pin->fd == -1)
		if (util_poolset_file(pin, 0, 0))
			return -1;

	if (pin->hdr == NULL || pin->hdrsize == 0) {
		if (util_map_hdr(pin, MAP_SHARED) != 0)
			return -1;

		/* set up uuid field in structure */
		struct pool_hdr *hdrp = pin->hdr;
		memcpy(pin->uuid, hdrp->uuid, POOL_HDR_UUID_LEN);
	}
	return 0;
}

/*
 * map_copy_header -- map headers and copy one into the other
 */
static int
map_copy_header(struct pool_set_part *pin, struct pool_set_part *pout)
{
	if (util_map_hdr(pin, MAP_SHARED) != 0)
		return -1;

	if (util_map_hdr(pout, MAP_SHARED) != 0) {
		return -1;
	}
	memcpy(pout->hdr, pin->hdr, POOL_HDR_SIZE);

	/* set up uuid field in structure */
	struct pool_hdr *hdrp = pout->hdr;
	memcpy(pout->uuid, hdrp->uuid, POOL_HDR_UUID_LEN);

	return 0;
}

/*
 * update_prev_part_uuid -- update previous uuid in given part
 */
static void
update_prev_part_uuid(struct pool_set_part *part, uuid_t *uuid)
{
	struct pool_hdr *hdrp = part->hdr;
	memcpy(hdrp->prev_part_uuid, uuid, POOL_HDR_UUID_LEN);
	util_checksum(hdrp, sizeof(*hdrp), &hdrp->checksum, 1);
}

/*
 * update_next_part_uuid -- update next uuid in given part
 */
static void
update_next_part_uuid(struct pool_set_part *part, uuid_t *uuid)
{
	struct pool_hdr *hdrp = part->hdr;
	memcpy(hdrp->next_part_uuid, uuid, POOL_HDR_UUID_LEN);
	util_checksum(hdrp, sizeof(*hdrp), &hdrp->checksum, 1);
}

/*
 * create_new_headers -- create new headers given range of parts in replica
 */
static int
create_new_headers(struct pool_set *set, unsigned repl,
		unsigned start, unsigned end)
{
	for (unsigned i = start; i <= end; ++i) {
		if (util_uuid_generate(set->replica[repl]->part[i].uuid) < 0)
			return -1;
	}

	struct pool_set_part *first_out_part =
			&set->replica[repl]->part[start - 1];
	struct pool_hdr *hdrp = first_out_part->hdr;
	memcpy(set->uuid, hdrp->poolset_uuid, POOL_HDR_UUID_LEN);

	for (unsigned i = start; i <= end; ++i) {
		struct pool_set_part *part = &set->replica[repl]->part[i];
		if (util_map_hdr(part, MAP_SHARED) != 0)
			return -1;

		if (util_header_create(set, repl, i,
				OBJ_HDR_SIG, OBJ_FORMAT_MAJOR,
				OBJ_FORMAT_COMPAT, OBJ_FORMAT_INCOMPAT,
				OBJ_FORMAT_RO_COMPAT, hdrp->prev_repl_uuid,
				hdrp->next_repl_uuid, NULL) != 0) {
			return -1;
		}
	}
	return 0;
}

/*
 * copy_headers_more_parts -- copy headers when number of output parts
 * is greater then input parts
 */
static int
copy_headers_more_parts(struct pool_replica *repl_in,
		struct pool_set *set_out, unsigned repl_out, struct region *reg)
{
	unsigned in_cnt = reg->part_first_in;
	unsigned out_cnt = reg->part_first_out;
	struct pool_replica *repl = set_out->replica[repl_out];

	while (in_cnt < reg->part_last_in) {
		if (map_copy_header(&repl_in->part[in_cnt],
				&repl->part[out_cnt]))
			return -1;
		in_cnt++;
		out_cnt++;
	}

	if (reg->part_first_in == reg->part_last_in) {
		/* copy the only one part from input side of region */
		if (map_copy_header(&repl_in->part[in_cnt],
				&repl->part[out_cnt]))
			return -1;

		struct pool_set_part *npart = &PART(repl,
				reg->part_last_out + 1);
		if (open_map_header(npart))
				return -1;

		if (create_new_headers(set_out, repl_out, out_cnt + 1,
				reg->part_last_out))
			return -1;

		/* update first part from output side of region */
		struct pool_hdr *hdrp = repl->
				part[reg->part_first_out + 1].hdr;
		update_next_part_uuid(&repl->part[reg->part_first_out],
				&hdrp->uuid);

		/* update uuid of next part to the last from output region */
		hdrp = repl->part[reg->part_last_out].hdr;
		update_prev_part_uuid(npart, &hdrp->uuid);

	} else {
		/* copy last element from input region */
		if (map_copy_header(&repl_in->part[reg->part_last_in],
				&repl->part[reg->part_last_out]))
			return -1;

		if (create_new_headers(set_out, repl_out, out_cnt,
				reg->part_last_out - 1))
			return -1;

		/*
		 * update parts next to newly created from output
		 * side of region
		 */
		struct pool_hdr *hdrp = repl->part[out_cnt].hdr;
		update_next_part_uuid(&repl->part[out_cnt - 1], &hdrp->uuid);

		hdrp = repl->part[reg->part_last_out - 1].hdr;
		update_prev_part_uuid(&repl->part[reg->part_last_out],
				&hdrp->uuid);
	}

	return 0;
}

/*
 * copy_headers_same_parts -- copy headers when number of output parts
 * is lesser then input parts
 */
static int
copy_headers_less_parts(struct pool_replica *repl_in,
		struct pool_set *set_out, unsigned repl_out, struct region *reg)
{
	unsigned in_cnt = reg->part_first_in;
	unsigned out_cnt = reg->part_first_out;
	struct pool_replica *repl = set_out->replica[repl_out];

	while (out_cnt <= reg->part_last_out) {
		if (out_cnt < reg->part_last_out) {
			/* copy corresponding header from input region */
			if (map_copy_header(&repl_in->part[in_cnt],
					&repl->part[out_cnt]))
				return -1;
		} else {
			if (reg->part_first_out == reg->part_last_out) {
				/*
				 * only one part in output region - copy first
				 * header from input region and update
				 * next part uuids
				 */
				if (map_copy_header(&repl_in->part[in_cnt],
						&repl->part[out_cnt]))
					return -1;

				struct pool_set_part *npart = &PART(repl,
						out_cnt + 1);
				if (open_map_header(npart))
					return -1;

				struct pool_hdr *hdrp = repl->part[out_cnt].hdr;
				update_prev_part_uuid(npart, &hdrp->uuid);

				hdrp = npart->hdr;
				update_next_part_uuid(&repl->part[out_cnt],
						&hdrp->uuid);
			} else {
				/* copy last header from input region */
				if (map_copy_header(&repl_in->
					part[reg->part_last_in],
					&repl->part[reg->part_last_out]))
						return -1;

				struct pool_hdr *hdrp = repl->
					part[out_cnt - 1].hdr;
				update_prev_part_uuid(&repl->part[out_cnt],
						&hdrp->uuid);

				hdrp = repl->part[out_cnt].hdr;
				update_next_part_uuid(&repl->part[out_cnt - 1],
					&hdrp->uuid);
			}
		}
		in_cnt++;
		out_cnt++;
	}
	return 0;
}

/*
 * copy_headers_same_parts -- copy headers when number of input and
 * output parts is the same in region
 */
static int
copy_headers_same_parts(struct pool_replica *repl_in,
		struct pool_set *set_out, unsigned repl_out, struct region *reg)
{
	unsigned in_cnt = reg->part_first_in;
	unsigned out_cnt = reg->part_first_out;
	while (in_cnt <= reg->part_last_in) {

		if (map_copy_header(&repl_in->part[in_cnt],
				&set_out->replica[repl_out]->part[out_cnt]))
			return -1;

		in_cnt++;
		out_cnt++;
	}
	return 0;
}

/*
 * copy_headers -- copy headers to created files
 */
static int
copy_headers(struct pool_set *set_in, struct pool_set *set_out,
		struct transform_context *trans_ctx)
{
	int res = 0;
	for (unsigned i = 0; i < trans_ctx->region_no; ++i) {
		struct region *curr_reg = &trans_ctx->region_list[i];
		unsigned part_no_in = curr_reg->part_last_in -
				curr_reg->part_first_in;
		unsigned part_no_out = curr_reg->part_last_out -
				curr_reg->part_first_out;

		struct pool_replica *repl_in = set_in->
				replica[curr_reg->replica];

		if (part_no_in == part_no_out)
			res = copy_headers_same_parts(repl_in, set_out,
					curr_reg->replica, curr_reg);
		else if (part_no_in < part_no_out)
			res = copy_headers_more_parts(repl_in, set_out,
					curr_reg->replica, curr_reg);
		else
			res = copy_headers_less_parts(repl_in, set_out,
					curr_reg->replica, curr_reg);

		if (res)
			return -1;
	}
	return 0;
}

/*
 * process_input_parts -- rename or remove input parts
 */
static int
process_input_parts(struct pool_set *set_in,
		struct transform_context *trans_ctx, unsigned flags)
{
	for (unsigned i = 0; i < trans_ctx->region_no; ++i) {
		struct region *reg = &trans_ctx->region_list[i];
		if (is_keep_orig(flags)) {
			if (rename_parts(set_in, reg->replica,
				reg->part_first_in, reg->part_last_in + 1,
				COPY_FILE_SUFFIX))
					return -1;
		} else {
			if (remove_parts(set_in, reg->replica,
				reg->part_first_in, reg->part_last_in + 1))
					return -1;
		}
	}

	return 0;
}

/*
 * grant_files_permission -- grant permissions for all output parts from
 * transformation context
 */
static int
grant_files_permission(struct pool_set *set_out,
		struct transform_context *trans_ctx)
{
	for (unsigned i = 0; i < trans_ctx->region_no; ++i) {
		struct region *reg = &trans_ctx->region_list[i];
		struct pool_replica *repl = set_out->replica[reg->replica];
		grant_part_perm(repl, reg->part_first_out,
				reg->part_last_out + 1);
	}
	return 0;
}

/*
 * rename_created_files -- rename created parts to final name
 */
static int
rename_created_files(struct pool_set *set_out,
		struct transform_context *trans_ctx)
{
	char *path_suffix = NULL;
	for (unsigned i = 0; i < trans_ctx->region_no; ++i) {
		struct region *reg = &trans_ctx->region_list[i];

		for (unsigned p = reg->part_first_out;
				p <= reg->part_last_out; ++p) {
			struct pool_set_part *part =
				&set_out->replica[reg->replica]->part[p];
			path_suffix = concatenate_str(part->path,
					TEMP_FILE_SUFFIX);
			if (!path_suffix)
				return -1;

			if (rename(path_suffix, part->path))
				return -1;
		}
	}
	return 0;
}

/*
 * transform_replica -- transforming one poolset into another
 */
int
transform_replica(struct pool_set *set_in, struct pool_set *set_out,
		const unsigned flags)
{
	int result = 0;
	struct transform_context transform_ctx;
	transform_ctx.region_list = NULL;
	transform_ctx.region_no = 0;

	/* checks poolsets */
	if (verify_arguments(set_in, set_out, flags))
		return -1;

	/* find regions to process for all replicas */
	for (unsigned i = 0; i < set_in->nreplicas; ++i) {
		if (find_regions(set_in, set_out, i, &transform_ctx)) {
			result = -1;
			ERR("Cannot find valid differences in poolsets");
			errno = EINVAL;
			goto error;
		}
	}

	/* open parts from input regions */
	if (open_parts_input_region(set_in, &transform_ctx)) {
		result = -1;
		goto error_repl;
	}

	/* if dry run after checking poolsets exit normally */
	if (is_dry_run(flags)) {
		result = 0;
		goto error_repl;
	}

	/* create temp files */
	if (create_poolset_temp_files(set_out, &transform_ctx)) {
		result = -1;
		goto error_repl;
	}

	/* map all regions from existing and output poolset */
	if (map_in_out_parts(set_in, set_out, &transform_ctx)) {
		result = -1;
		goto error_repl;
	}

	/* copy all data */
	if (copy_data(set_in, set_out, &transform_ctx)) {
		result = -1;
		goto error_repl;
	}

	/* copy all headers */
	if (copy_headers(set_in, set_out, &transform_ctx)) {
		result = -1;
		goto error_repl;
	}

	/* remove transformed files from input poolset */
	if (process_input_parts(set_in, &transform_ctx, flags)) {
		result = -1;
		goto error_repl;
	}

	/* change name of output files to final one */
	if (rename_created_files(set_out, &transform_ctx)) {
		result = -1;
		goto error_repl;
	}

	/* grant rwx permissions */
	if (grant_files_permission(set_out, &transform_ctx)) {
		result = -1;
		goto error_repl;
	}

error_repl:
	close_poolset_replicas(set_in, set_out, &transform_ctx);

error:
	if (!transform_ctx.region_list)
		free(transform_ctx.region_list);
	return result;
}
