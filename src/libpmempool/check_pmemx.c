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

#include <assert.h>
#include <inttypes.h>
#include <sys/param.h>

#include "out.h"
#include "util.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check.h"
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
 * check_pmemx_log_convert2h -- convert pmemlog structure to host byte order
 */
static void
check_pmemx_log_convert2h(struct pmemlog *plp)
{
	plp->start_offset = le64toh(plp->start_offset);
	plp->end_offset = le64toh(plp->end_offset);
	plp->write_offset = le64toh(plp->write_offset);
}

/*
 * check_pmemx_log_read -- read pmemlog header
 */
static struct check_status *
check_pmemx_log_read(PMEMpoolcheck *ppc)
{
	/*
	 * Here we want to read the pmemlog header
	 * without the pool_hdr as we've already done it
	 * before.
	 *
	 * Take the pointer to fields right after pool_hdr,
	 * compute the size and offset of remaining fields.
	 */
	uint8_t *ptr = (uint8_t *)&ppc->pool->hdr.log;
	ptr += sizeof (ppc->pool->hdr.log.hdr);

	size_t size = sizeof (ppc->pool->hdr.log) -
		sizeof (ppc->pool->hdr.log.hdr);
	uint64_t offset = sizeof (ppc->pool->hdr.log.hdr);

	if (pool_read(ppc->pool->set_file, ptr, size, offset)) {
		return CHECK_STATUS_ERR(ppc, "cannot read pmemlog structure");
	}

	/* endianness conversion */
	check_pmemx_log_convert2h(&ppc->pool->hdr.log);

	return NULL;
}

/*
 * check_pmemx_log -- check pmemlog header
 */
static struct check_status *
check_pmemx_log(PMEMpoolcheck *ppc, union check_pmemx_location *loc)
{
	LOG(2, "checking pmemlog header\n");

	static struct check_status *status = NULL;
	if ((status = check_pmemx_log_read(ppc)) != NULL) {
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
check_pmemx_log_fix(PMEMpoolcheck *ppc, union check_pmemx_location *loc)
{
	if (!check_has_answer(ppc->data))
		return NULL;

	static struct check_status *status = NULL;
	if ((status = check_pmemx_log_read(ppc)) != NULL) {
		ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
		return status;
	}

	/* determine constant values for pmemlog */
	const uint64_t d_start_offset =
		roundup(sizeof (ppc->pool->hdr.log), LOG_FORMAT_DATA_ALIGN);
	struct check_status *answer;

	while ((answer = check_pop_answer(ppc->data)) != NULL) {
		if (answer->status.answer == PMEMPOOL_CHECK_ANSWER_YES) {
			switch (answer->status.question) {
			case CHECK_PMEMX_Q_LOG_START_OFFSET:
				LOG(1, "setting pmemlog.start_offset to 0x%"
					PRIx64, d_start_offset);
				ppc->pool->hdr.log.start_offset =
					d_start_offset;
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
			}
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_REPAIRED;
		} else {
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return CHECK_STATUS_ERR(ppc, "");
		}
	}

	return NULL;
}

/*
 * check_pmemx_blk_read -- read pmemblk header
 */
static struct check_status *
check_pmemx_blk_read(PMEMpoolcheck *ppc)
{
	/*
	 * Here we want to read the pmemlog header
	 * without the pool_hdr as we've already done it
	 * before.
	 *
	 * Take the pointer to fields right after pool_hdr,
	 * compute the size and offset of remaining fields.
	 */
	uint8_t *ptr = (uint8_t *)&ppc->pool->hdr.blk;
	ptr += sizeof (ppc->pool->hdr.blk.hdr);

	size_t size = sizeof (ppc->pool->hdr.blk) -
		sizeof (ppc->pool->hdr.blk.hdr);
	uint64_t offset = sizeof (ppc->pool->hdr.blk.hdr);

	if (pool_read(ppc->pool->set_file, ptr, size, offset)) {
		return CHECK_STATUS_ERR(ppc, "cannot read pmemblk structure");
	}

	/* endianness conversion */
	ppc->pool->hdr.blk.bsize = le32toh(ppc->pool->hdr.blk.bsize);

	return NULL;
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
	assert(internal_lbasize <= UINT32_MAX);

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
	if ((status = check_pmemx_blk_read(ppc)) != NULL) {
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
check_pmemx_blk_fix(PMEMpoolcheck *ppc, union check_pmemx_location *loc)
{
	if (!check_has_answer(ppc->data))
		return NULL;

	static struct check_status *status = NULL;
	if ((status = check_pmemx_blk_read(ppc)) != NULL) {
		ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
		return status;
	}

	/* check for valid BTT Info arena as we can take bsize from it */
	if (!ppc->pool->bttc.valid)
		pool_get_first_valid_arena(ppc->pool->set_file,
			&ppc->pool->bttc);
	struct check_status *answer;
	uint32_t btt_bsize;

	while ((answer = check_pop_answer(ppc->data)) != NULL) {
		if (answer->status.answer == PMEMPOOL_CHECK_ANSWER_YES) {
			switch (answer->status.question) {
			case CHECK_PMEMX_Q_BLK_BSIZE:
				btt_bsize = ppc->pool->bttc.btt_info.
					external_lbasize;
				LOG(1, "setting pmemblk.b_size to 0x%" PRIx32,
					btt_bsize);
				ppc->pool->hdr.blk.bsize = btt_bsize;
				break;
			}
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_REPAIRED;
		} else {
			check_status_release(answer);
			ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return CHECK_STATUS_ERR(ppc, "");
		}
	}

	LOG(2, "pmemblk header correct");
	return NULL;
}

struct check_pmemx_step {
	struct check_status *(*func)(PMEMpoolcheck *,
		union check_pmemx_location *loc);
	enum pool_type type;
};

static const struct check_pmemx_step check_pmemx_steps[] = {
	{
		.func	= check_pmemx_log,
		.type	= POOL_TYPE_LOG
	},
	{
		.func	= check_pmemx_log_fix,
		.type	= POOL_TYPE_LOG
	},
	{
		.func	= check_pmemx_blk,
		.type	= POOL_TYPE_BLK
	},
	{
		.func	= check_pmemx_blk_fix,
		.type	= POOL_TYPE_BLK
	},
	{
		.func	= NULL,
	},
};

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
		check_pmemx_steps[loc->step].func != NULL) {
		const struct check_pmemx_step *step =
			&check_pmemx_steps[loc->step++];

		if (!(step->type & ppc->pool->params.type))
			continue;

		status = step->func(ppc, loc);
		if (status != NULL)
			goto cleanup_return;
	}

cleanup_return:
	return status;
}
