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
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check_util.h"
#include "check_pmemx.h"

union location {
	struct {
		uint32_t step;
	};
	struct check_instep instep;
};

enum question {
	Q_LOG_START_OFFSET,
	Q_LOG_END_OFFSET,
	Q_LOG_WRITE_OFFSET,
	Q_BLK_BSIZE,
};

/*
 * log_convert2h -- convert pmemlog structure to host byte order
 */
static void
log_convert2h(struct pmemlog *plp)
{
	plp->start_offset = le64toh(plp->start_offset);
	plp->end_offset = le64toh(plp->end_offset);
	plp->write_offset = le64toh(plp->write_offset);
}

/*
 * log_read -- read pmemlog header
 */
static int
log_read(PMEMpoolcheck *ppc)
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
		return CHECK_ERR(ppc, "cannot read pmemlog structure");
	}

	/* endianness conversion */
	log_convert2h(&ppc->pool->hdr.log);
	return 0;
}

/*
 * log_check -- check pmemlog header
 */
static int
log_hdr_check(PMEMpoolcheck *ppc, union location *loc)
{
	CHECK_INFO(ppc, "checking pmemlog header");

	if (log_read(ppc)) {
		ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
		return -1;
	}

	/* determine constant values for pmemlog */
	const uint64_t d_start_offset =
		roundup(sizeof (ppc->pool->hdr.log), LOG_FORMAT_DATA_ALIGN);

	if (ppc->pool->hdr.log.start_offset != d_start_offset) {
		if (CHECK_ASK(ppc, Q_LOG_START_OFFSET,
			"invalid pmemlog.start_offset: 0x%" PRIx64 ".|Do you "
			" want to set pmemlog.start_offset to default 0x%x?",
			ppc->pool->hdr.log.start_offset, d_start_offset))
			goto error;

	}

	if (ppc->pool->hdr.log.end_offset != ppc->pool->set_file->size) {
		if (CHECK_ASK(ppc, Q_LOG_END_OFFSET,
			"invalid pmemlog.end_offset: 0x%" PRIx64 ".|Do you "
			"want to set pmemlog.end_offset to 0x%x?",
			ppc->pool->hdr.log.end_offset,
			ppc->pool->set_file->size))
			goto error;
	}

	if (ppc->pool->hdr.log.write_offset < d_start_offset ||
		ppc->pool->hdr.log.write_offset > ppc->pool->set_file->size) {
		if (CHECK_ASK(ppc, Q_LOG_WRITE_OFFSET,
			"invalid pmemlog.write_offset: 0x%" PRIx64 ".|Do you "
			"want to set pmemlog.write_offset to "
			"pmemlog.end_offset?",
			ppc->pool->hdr.log.write_offset))
			goto error;
	}

	if (ppc->result == PMEMPOOL_CHECK_RESULT_CONSISTENT ||
		ppc->result == PMEMPOOL_CHECK_RESULT_REPAIRED)
		CHECK_INFO(ppc, "pmemlog header correct");

	return check_questions_sequence_validate(ppc);

error:
	ppc->result = PMEMPOOL_CHECK_RESULT_NOT_CONSISTENT;
	return -1;
}

/*
 * log_hdr_fix -- fix pmemlog header
 */
static int
log_hdr_fix(PMEMpoolcheck *ppc, struct check_instep *location,
	uint32_t question, void *ctx)
{
	uint64_t d_start_offset;

	switch (question) {
	case Q_LOG_START_OFFSET:
		/* determine constant values for pmemlog */
		d_start_offset = roundup(sizeof (ppc->pool->hdr.log),
			LOG_FORMAT_DATA_ALIGN);
		CHECK_INFO(ppc, "setting pmemlog.start_offset to 0x%" PRIx64,
			d_start_offset);
		ppc->pool->hdr.log.start_offset = d_start_offset;
		break;
	case Q_LOG_END_OFFSET:
		CHECK_INFO(ppc, "setting pmemlog.end_offset to 0x%" PRIx64,
			ppc->pool->set_file->size);
		ppc->pool->hdr.log.end_offset = ppc->pool->set_file->size;
			break;
	case Q_LOG_WRITE_OFFSET:
		CHECK_INFO(ppc, "setting pmemlog.write_offset to "
			"pmemlog.end_offset");
		ppc->pool->hdr.log.write_offset = ppc->pool->set_file->size;
		break;
	default:
		ERR("not implemented question id: %u", question);
	}

	return 0;
}

/*
 * blk_get_max_bsize -- return maximum size of block for given file size
 */
static uint32_t
blk_get_max_bsize(uint64_t fsize)
{
	if (fsize == 0)
		return 0;

	/* default nfree */
	uint32_t nfree = BTT_DEFAULT_NFREE;

	/* number of blocks must be at least 2 * nfree */
	uint32_t internal_nlba = 2 * nfree;

	/* compute flog size */
	uint32_t flog_size = nfree * (uint32_t)roundup(2 *
		sizeof (struct btt_flog), BTT_FLOG_PAIR_ALIGN);
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

	internal_lbasize = roundup(internal_lbasize, BTT_INTERNAL_LBA_ALIGNMENT)
		- BTT_INTERNAL_LBA_ALIGNMENT;

	return (uint32_t)internal_lbasize;
}

/*
 * blk_read -- read pmemblk header
 */
static int
blk_read(PMEMpoolcheck *ppc)
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
		return CHECK_ERR(ppc, "cannot read pmemblk structure");
	}

	/* endianness conversion */
	ppc->pool->hdr.blk.bsize = le32toh(ppc->pool->hdr.blk.bsize);

	return 0;
}

/*
 * check_pmemx_blk_bsize -- check if block size is valid for given file size
 */
static int
blk_bsize(uint32_t bsize, uint64_t fsize)
{
	uint32_t max_bsize = blk_get_max_bsize(fsize);
	return !(bsize < max_bsize);
}

/*
 * blk_hdr_check -- check pmemblk header
 */
static int
blk_hdr_check(PMEMpoolcheck *ppc, union location *loc)
{
	CHECK_INFO(ppc, "checking pmemblk header");

	if (blk_read(ppc)) {
		ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
		return -1;
	}

	/* check for valid BTT Info arena as we can take bsize from it */
	if (!ppc->pool->bttc.valid)
		pool_get_first_valid_arena(ppc->pool->set_file,
			&ppc->pool->bttc);

	if (ppc->pool->bttc.valid) {
		const uint32_t btt_bsize =
			ppc->pool->bttc.btt_info.external_lbasize;

		if (ppc->pool->hdr.blk.bsize != btt_bsize) {
			CHECK_ASK(ppc, Q_BLK_BSIZE,
				"invalid pmemblk.bsize.|Do you want to set "
				"pmemblk.bsize to %lu from BTT Info?",
				btt_bsize);
		}
	} else if (ppc->pool->bttc.zeroed) {
		CHECK_INFO(ppc, "no BTT layout");
	} else {
		if (ppc->pool->hdr.blk.bsize < BTT_MIN_LBA_SIZE ||
			blk_bsize(ppc->pool->hdr.blk.bsize,
			ppc->pool->set_file->size)) {
			ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
			return CHECK_ERR(ppc, "invalid pmemblk.bsize");
		}
	}

	if (ppc->result == PMEMPOOL_CHECK_RESULT_CONSISTENT ||
		ppc->result == PMEMPOOL_CHECK_RESULT_REPAIRED)
		CHECK_INFO(ppc, "pmemblk header correct");

	return check_questions_sequence_validate(ppc);
}

/*
 * blk_hdr_fix -- fix pmemblk header
 */
static int
blk_hdr_fix(PMEMpoolcheck *ppc, struct check_instep *location,
	uint32_t question, void *ctx)
{
	uint32_t btt_bsize;

	switch (question) {
	case Q_BLK_BSIZE:
		/*
		 * check for valid BTT Info arena as we can take bsize from it
		 */
		if (!ppc->pool->bttc.valid)
			pool_get_first_valid_arena(ppc->pool->set_file,
				&ppc->pool->bttc);
		btt_bsize = ppc->pool->bttc.btt_info.external_lbasize;
		CHECK_INFO(ppc, "setting pmemblk.b_size to 0x%" PRIx32,
			btt_bsize);
		ppc->pool->hdr.blk.bsize = btt_bsize;
		break;
	default:
		ERR("not implemented question id: %u", question);
	}

	return 0;
}

struct step {
	int (*check)(PMEMpoolcheck *, union location *loc);
	int (*fix)(PMEMpoolcheck *ppc, struct check_instep *location,
		uint32_t question, void *ctx);
	enum pool_type type;
};

static const struct step steps[] = {
	{
		.check	= log_hdr_check,
		.type	= POOL_TYPE_LOG
	},
	{
		.fix	= log_hdr_fix,
		.type	= POOL_TYPE_LOG
	},
	{
		.check	= blk_hdr_check,
		.type	= POOL_TYPE_BLK
	},
	{
		.fix	= blk_hdr_fix,
		.type	= POOL_TYPE_BLK
	},
	{
		.check	= NULL,
	},
};

/*
 * check_pmemx_step -- perform single step according to its parameters
 */
static inline int
check_pmemx_step(PMEMpoolcheck *ppc, union location *loc)
{
	const struct step *step = &steps[loc->step++];

	if (!(step->type & ppc->pool->params.type))
		return 0;

	int status = 0;
	if (step->fix != NULL) {
		if (!check_has_answer(ppc->data))
			return 0;

		if (step->type == POOL_TYPE_LOG) {
			if (log_read(ppc)) {
				ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
				return -1;
			}
		} else if (step->type == POOL_TYPE_BLK) {
			/*
			 * blk related questions require blk preparation
			 */
			if (blk_read(ppc)) {
				ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
				return -1;
			}
		}

		status = check_answer_loop(ppc, (struct check_instep *)loc,
			NULL, step->fix);
	} else
		status = step->check(ppc, loc);

	return status;
}

/*
 * check_pmemx -- entry point for pmemlog and pmemblk checks
 */
void
check_pmemx(PMEMpoolcheck *ppc)
{
	COMPILE_ERROR_ON(sizeof (union location) !=
		sizeof (struct check_instep));

	union location *loc =
		(union location *)check_step_location_get(ppc->data);

	while (loc->step != CHECK_STEP_COMPLETE &&
		(steps[loc->step].check != NULL ||
		steps[loc->step].fix != NULL)) {

		if (check_pmemx_step(ppc, loc))
			break;
	}
}
