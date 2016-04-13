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
 * check_pmemx.c -- check pmemlog and pmemblk
 */

#include <inttypes.h>
#include <sys/param.h>

#include "out.h"
#include "util.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check.h"
#include "check_utils.h"
#include "check_pmemx.h"

union check_pmemx_location {
	struct {
		uint32_t step;
	};
	struct check_instep_location instep;
};

enum check_pmemx_questions {
	CHECK_PMEMX_Q_LOG_START_OFFSET,
	CHECK_PMEMX_Q_LOG_END_OFFSET,
	CHECK_PMEMX_Q_LOG_WRITE_OFFSET,
	CHECK_PMEMX_Q_BLK_BSIZE,
};

/*
 * check_pmemx_log -- check pmemlog header
 */
static struct check_status *
check_pmemx_log(PMEMpoolcheck *ppc, union check_pmemx_location *loc)
{
	LOG(2, "checking pmemlog header\n");

	static struct check_status *status = NULL;
	if ((status = check_utils_log_read(ppc)) != NULL) {
		ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
		return status;
	}

	/* determine constant values for pmemlog */
	const uint64_t d_start_offset =
		roundup(sizeof (ppc->pool->hdr.log), LOG_FORMAT_DATA_ALIGN);

	if (ppc->pool->hdr.log.start_offset != d_start_offset) {
		CHECK_STATUS_ASK(ppc, CHECK_PMEMX_Q_LOG_START_OFFSET,
			"invalid pmemlog.start_offset: 0x%" PRIx64 ". Do you "
			" want to set pmemlog.start_offset to default 0x%x?",
			ppc->pool->hdr.log.start_offset, d_start_offset);
	}

	if (ppc->pool->hdr.log.end_offset != ppc->pool->set_file->size) {
		CHECK_STATUS_ASK(ppc, CHECK_PMEMX_Q_LOG_END_OFFSET,
			"invalid pmemlog.end_offset: 0x%" PRIx64 ". Do you "
			"want to set pmemlog.end_offset to 0x%x?",
			ppc->pool->hdr.log.end_offset,
			ppc->pool->set_file->size);
	}

	if (ppc->pool->hdr.log.write_offset < d_start_offset ||
		ppc->pool->hdr.log.write_offset > ppc->pool->set_file->size) {
		CHECK_STATUS_ASK(ppc, CHECK_PMEMX_Q_LOG_WRITE_OFFSET,
			"invalid pmemlog.write_offset: 0x%" PRIx64 ". Do you "
			"want to set pmemlog.write_offset to "
			"pmemlog.end_offset?",
			ppc->pool->hdr.log.write_offset);
	}

	return check_questions_sequence_validate(ppc);
}

/*
 * check_pmemx_log_fix -- fix pmemlog header
 */
static struct check_status *
check_pmemx_log_fix(PMEMpoolcheck *ppc,
	struct check_instep_location *location, uint32_t question, void *ctx)
{
	struct check_status *result = NULL;
	uint64_t d_start_offset;

	switch (question) {
	case CHECK_PMEMX_Q_LOG_START_OFFSET:
		/* determine constant values for pmemlog */
		d_start_offset = roundup(sizeof (ppc->pool->hdr.log),
			LOG_FORMAT_DATA_ALIGN);
		LOG(1, "setting pmemlog.start_offset to 0x%" PRIx64,
			d_start_offset);
		ppc->pool->hdr.log.start_offset = d_start_offset;
		break;
	case CHECK_PMEMX_Q_LOG_END_OFFSET:
		LOG(1, "setting pmemlog.end_offset to 0x%"
			PRIx64, ppc->pool->set_file->size);
		ppc->pool->hdr.log.end_offset =
			ppc->pool->set_file->size;
			break;
	case CHECK_PMEMX_Q_LOG_WRITE_OFFSET:
		LOG(1, "setting pmemlog.write_offset to "
			"pmemlog.end_offset");
		ppc->pool->hdr.log.write_offset =
			ppc->pool->set_file->size;
		break;
	default:
		FATAL("not implemented");
	}

	return result;
}

/*
 * check_pmemx_blk_get_max_bsize -- return maximum size of block for given file
 *	size
 */
static uint32_t
check_pmemx_blk_get_max_bsize(uint64_t fsize)
{
	if (fsize == 0)
		return 0;

	/* default nfree */
	uint32_t nfree = BTT_DEFAULT_NFREE;

	/* number of blocks must be at least 2 * nfree */
	uint32_t internal_nlba = 2 * nfree;

	/* compute flog size */
	uint32_t flog_size = nfree *
		(uint32_t)roundup(2 * sizeof (struct btt_flog),
				BTT_FLOG_PAIR_ALIGN);
	flog_size = (uint32_t)roundup(flog_size, BTT_ALIGNMENT);

	/* compute arena size from file size */
	uint64_t arena_size = fsize;
	/* without pmemblk structure */
	arena_size -= sizeof (struct pmemblk);
	if (arena_size > BTT_MAX_ARENA) {
		arena_size = BTT_MAX_ARENA;
	}
	/* without BTT Info header and backup */
	arena_size -= 2 * sizeof (struct btt_info);
	/* without BTT FLOG size */
	arena_size -= flog_size;

	/* compute maximum internal LBA size */
	uint64_t internal_lbasize = (arena_size - BTT_ALIGNMENT) /
			internal_nlba - BTT_MAP_ENTRY_SIZE;
	ASSERT(internal_lbasize <= UINT32_MAX);

	if (internal_lbasize < BTT_MIN_LBA_SIZE)
		internal_lbasize = BTT_MIN_LBA_SIZE;

	internal_lbasize =
		roundup(internal_lbasize, BTT_INTERNAL_LBA_ALIGNMENT)
			- BTT_INTERNAL_LBA_ALIGNMENT;

	return (uint32_t)internal_lbasize;
}

/*
 * check_pmemx_blk_bsize -- check if block size is valid for given file size
 */
static int
check_pmemx_blk_bsize(uint32_t bsize, uint64_t fsize)
{
	uint32_t max_bsize = check_pmemx_blk_get_max_bsize(fsize);
	return !(bsize < max_bsize);
}

/*
 * check_pmemx_blk -- check pmemblk header
 */
static struct check_status *
check_pmemx_blk(PMEMpoolcheck *ppc, union check_pmemx_location *loc)
{
	LOG(2, "checking pmemblk header\n");

	static struct check_status *status = NULL;
	if ((status = check_utils_blk_read(ppc)) != NULL) {
		ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
		return status;
	}

	/* check for valid BTT Info arena as we can take bsize from it */
	if (!ppc->pool->bttc.valid)
		pool_get_first_valid_arena(ppc->pool->set_file,
			&ppc->pool->bttc);

	if (ppc->pool->bttc.valid) {
		const uint32_t btt_bsize =
			ppc->pool->bttc.btt_info.external_lbasize;

		if (ppc->pool->hdr.blk.bsize != btt_bsize) {
			CHECK_STATUS_ASK(ppc, CHECK_PMEMX_Q_BLK_BSIZE,
				"invalid pmemblk.bsize. Do you want to set "
				"pmemblk.bsize to %lu from BTT Info?",
				btt_bsize);
		}
	} else if (ppc->pool->bttc.zeroed) {
		LOG(2, "no BTT layout\n");
	} else {
		if (ppc->pool->hdr.blk.bsize < BTT_MIN_LBA_SIZE ||
			check_pmemx_blk_bsize(ppc->pool->hdr.blk.bsize,
				ppc->pool->set_file->size)) {
			ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return CHECK_STATUS_ERR(ppc, "invalid pmemblk.bsize");
		}
	}

	return check_questions_sequence_validate(ppc);
}

/*
 * check_pmemx_blk_fix -- fix pmemblk header
 */
static struct check_status *
check_pmemx_blk_fix(PMEMpoolcheck *ppc,
	struct check_instep_location *location, uint32_t question, void *ctx)
{
	struct check_status *result = NULL;
	uint32_t btt_bsize;

	switch (question) {
	case CHECK_PMEMX_Q_BLK_BSIZE:
		/*
		 * check for valid BTT Info arena as we can take bsize from
		 * it
		 */
		if (!ppc->pool->bttc.valid)
			pool_get_first_valid_arena(ppc->pool->set_file,
				&ppc->pool->bttc);
		btt_bsize = ppc->pool->bttc.btt_info.
			external_lbasize;
		LOG(1, "setting pmemblk.b_size to 0x%" PRIx32,
			btt_bsize);
		ppc->pool->hdr.blk.bsize = btt_bsize;
		break;
	default:
		FATAL("not implemented");
	}

	return result;
}

struct check_pmemx_step {
	struct check_status *(*check)(PMEMpoolcheck *,
		union check_pmemx_location *loc);
	struct check_status *(*fix)(PMEMpoolcheck *ppc,
		struct check_instep_location *location, uint32_t question,
		void *ctx);
	enum pool_type type;
};

static const struct check_pmemx_step check_pmemx_steps[] = {
	{
		.check	= check_pmemx_log,
		.type	= POOL_TYPE_LOG
	},
	{
		.fix	= check_pmemx_log_fix,
		.type	= POOL_TYPE_LOG
	},
	{
		.check	= check_pmemx_blk,
		.type	= POOL_TYPE_BLK
	},
	{
		.fix	= check_pmemx_blk_fix,
		.type	= POOL_TYPE_BLK
	},
	{
		.check	= NULL,
	},
};

/*
 * check_pmemx_step -- perform single step according to its parameters
 */
static inline struct check_status *
check_pmemx_step(PMEMpoolcheck *ppc, union check_pmemx_location *loc)
{
	const struct check_pmemx_step *step =
		&check_pmemx_steps[loc->step++];

	if (!(step->type & ppc->pool->params.type))
		return NULL;

	struct check_status *status = NULL;
	if (step->fix != NULL) {
		if (!check_has_answer(ppc->data))
				return NULL;

		if (step->type == POOL_TYPE_LOG) {
			if ((status = check_utils_log_read(ppc)) != NULL) {
				ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
				return status;
			}
		} else if (step->type == POOL_TYPE_BLK) {
			/*
			 * blk related questions require blk preparation
			 */
			if ((status = check_utils_blk_read(ppc)) != NULL) {
				ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
				return status;
			}
		}

		status = check_utils_answer_loop(ppc,
			(struct check_instep_location *)loc, NULL,
			step->fix);
	} else
		status = step->check(ppc, loc);

	return status;
}

/*
 * check_pmemx -- entry point for pmemlog and pmemblk checks
 */
struct check_status *
check_pmemx(PMEMpoolcheck *ppc)
{
	COMPILE_ERROR_ON(sizeof (union check_pmemx_location) !=
		sizeof (struct check_instep_location));

	union check_pmemx_location *loc =
		(union check_pmemx_location *)&ppc->data->instep_location;
	struct check_status *status = NULL;

	while (loc->step != CHECK_STEPS_COMPLETE &&
		(check_pmemx_steps[loc->step].check != NULL ||
		check_pmemx_steps[loc->step].fix != NULL)) {

		status = check_pmemx_step(ppc, loc);
		if (status != NULL)
			goto cleanup_return;
	}

cleanup_return:
	return status;
}
