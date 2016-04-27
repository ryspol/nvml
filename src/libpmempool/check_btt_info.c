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
 * check_btt_info.c -- check btt info
 */

#include <unistd.h>
#include <stdint.h>
#include <sys/mman.h>

#include "out.h"
#include "btt.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check_util.h"
#include "check_btt_info.h"

union location {
	struct {
		uint64_t offset;
		uint64_t offset2;
		uint64_t nextoff;
		struct arena *arena;
		bool advanced_repair;
		uint32_t step;
	};
	struct check_instep instep;
};

enum question {
	Q_RESTORE_BACKUP,
	Q_REGENERATE,
};

struct btt_context {
	PMEMpoolcheck *ppc;
	uint64_t base_off;
	uint64_t len;
};

/*
 * location_release -- (internal) release check_btt_info_loc allocations
 */
static void
location_release(union location *loc)
{
	free(loc->arena);
	loc->arena = NULL;
}

/*
 * btt_info_checksum -- (internal) check BTT Info checksum
 */
static int
btt_info_checksum(PMEMpoolcheck *ppc, union location *loc)
{
	int status = 0;
	loc->arena = calloc(1, sizeof (struct arena));
	if (!loc->arena) {
		ppc->result = PMEMPOOL_CHECK_RESULT_INTERNAL_ERROR;
		status = CHECK_ERR(ppc, "cannot allocate memory for arena");
		goto cleanup;
	}

	/* read the BTT Info header at well known offset */
	if (pool_read(ppc->pool, &loc->arena->btt_info,
		sizeof (loc->arena->btt_info), loc->offset) != 0) {
		status = CHECK_ERR(ppc, "arena %u: cannot read BTT Info header",
			loc->arena->id);
		goto error;
	}

	loc->arena->id = ppc->pool->narenas;

	if (check_memory((const uint8_t *)&loc->arena->btt_info,
		sizeof (loc->arena->btt_info), 0) == 0) {
		CHECK_INFO(ppc, "BTT Layout not written");
		ppc->pool->blk_no_layout = 1;
		loc->step = CHECK_STEP_COMPLETE;
		goto cleanup;
	}

	/* check consistency of BTT Info */
	int ret = pool_btt_info_valid(&loc->arena->btt_info);

	if (ret == 1) {
		CHECK_INFO(ppc, "arena %u: BTT Info header checksum correct",
			loc->arena->id);
		loc->step = CHECK_STEP_COMPLETE;
	} else {
		if (!ppc->args.repair) {
			status = CHECK_ERR(ppc,
				"arena %u: BTT Info header checksum incorrect",
				loc->arena->id);
			goto error;
		}
		/*
		 * if BTT Info is not consistent and we are allowed to fix it
		 * we do not return error status here and go to the next step
		 * which may fix this issue
		 */
	}

	return 0;

error:
	ppc->result = PMEMPOOL_CHECK_RESULT_ERROR;
cleanup:
	location_release(loc);
	return status;
}

/*
 * btt_info_backup -- (internal) check BTT Info backup
 */
static int
btt_info_backup(PMEMpoolcheck *ppc, union location *loc)
{
	ASSERT(ppc->args.repair);

	/*
	 * BTT Info header is not consistent, so try to find
	 * backup first.
	 *
	 * BTT Info header backup is in the last page of arena,
	 * we know the BTT Info size and arena maximum size so
	 * we can calculate theoretical backup offset.
	 */
	uint64_t endoff = (ppc->pool->set_file->size & ~(BTT_ALIGNMENT - 1));
	endoff = min(loc->offset + BTT_MAX_ARENA, endoff);
	loc->offset2 = endoff - sizeof (struct btt_info);

	/*
	 * Read first valid BTT Info to bttc buffer
	 * check whether this BTT Info header is the
	 * backup by checking offset value.
	 */
	if ((loc->offset2 = pool_get_valid_btt(ppc, &ppc->pool->bttc.btt_info,
		loc->offset2)) && loc->offset +
		ppc->pool->bttc.btt_info.infooff == loc->offset2) {
		/*
		 * Here we have valid BTT Info backup
		 * so we can restore it.
		 */
		CHECK_ASK(ppc, Q_RESTORE_BACKUP,
			"arena %u: BTT Info header checksum incorrect.|"
			"Restore BTT Info from backup?", loc->arena->id);
	} else {
		loc->advanced_repair = true;
		if (endoff < ppc->pool->set_file->size)
			loc->offset2 = endoff;
	}

	return check_questions_sequence_validate(ppc);
}

/*
 * btt_info_backup_fix -- (internal) fix BTT Info using its backup
 */
static int
btt_info_backup_fix(PMEMpoolcheck *ppc, struct check_instep *location,
	uint32_t question, void *ctx)
{
	ASSERTeq(ctx, NULL);
	ASSERTne(location, NULL);
	union location *loc = (union location *)location;

	switch (question) {
	case Q_RESTORE_BACKUP:
		CHECK_INFO(ppc,
			"arena %u: restoring BTT Info header from backup",
			loc->arena->id);

		memcpy(&loc->arena->btt_info, &ppc->pool->bttc.btt_info,
			sizeof (loc->arena->btt_info));
		loc->advanced_repair = false;
		break;
	default:
		ERR("not implemented question id: %u", question);
	}

	return 0;
}

/*
 * btt_info_gen -- (internal) ask whether try to regenerate BTT Info
 */
static int
btt_info_gen(PMEMpoolcheck *ppc, union location *loc)
{
	CHECK_ASK(ppc, Q_REGENERATE,
		"arena %u: BTT Info header checksum incorrect.|Do you want to "
		"restore BTT layout?", loc->arena->id);

	return check_questions_sequence_validate(ppc);
}

/*
 * ns_read -- (internal) btt callback for reading
 */
static int
ns_read(void *ns, unsigned lane, void *buf, size_t count, uint64_t off)
{
	struct btt_context *nsc = (struct btt_context *)ns;

	if (off + count > nsc->len) {
		errno = EINVAL;
		return -1;
	}
	pool_read(nsc->ppc->pool, buf, count, nsc->base_off + off);

	return 0;
}

/*
 * ns_write -- (internal) btt callback for writing
 */
static int
ns_write(void *ns, unsigned lane, const void *buf, size_t count, uint64_t off)
{
	struct btt_context *nsc = (struct btt_context *)ns;

	if (off + count > nsc->len) {
		errno = EINVAL;
		return -1;
	}
	pool_write(nsc->ppc->pool, buf, count, nsc->base_off + off);

	return 0;
}

/*
 * ns_map -- (internal) btt callback for memory mapping
 */
static ssize_t
ns_map(void *ns, unsigned lane, void **addrp, size_t len, uint64_t off)
{
	struct btt_context *nsc = (struct btt_context *)ns;

	ASSERT((ssize_t)len >= 0);

	if (off + len >= nsc->len) {
		errno = EINVAL;
		return -1;
	}

	/*
	 * Since the entire file is memory-mapped, this callback can always
	 * provide the entire length requested. For BTT device it is only an
	 * offset calculation.
	 */
	*addrp = (void *)(nsc->base_off + off);

	return (ssize_t)len;
}

/*
 * ns_sync -- (internal) btt callback for memory synchronization
 */
static void
ns_sync(void *ns, unsigned lane, void *addr, size_t len)
{
	/* do nothing */
}

/*
 * ns_zero -- (internal) btt callback for zeroing memory
 */
static int
ns_zero(void *ns, unsigned lane, size_t len, uint64_t off)
{
	struct btt_context *nsc = (struct btt_context *)ns;

	if (off + len >= nsc->len) {
		errno = EINVAL;
		return -1;
	}
	pool_memset(nsc->ppc->pool, nsc->base_off + off, 0, len);

	return 0;
}

/*
 * ns_callbacks -- callbacks for btt API
 */
static struct ns_callback ns_callbacks = {
	.nsread		= ns_read,
	.nswrite	= ns_write,
	.nsmap		= ns_map,
	.nssync		= ns_sync,
	.nszero		= ns_zero,
};

/*
 * btt_info_gen_fix_exe -- (internal) BTT Info regeneration
 */
static int
btt_info_gen_fix_exe(PMEMpoolcheck *ppc, union location *loc)
{
	bool eof = false;
	uint64_t startoff = loc->offset;
	uint64_t endoff = loc->offset2;
	int status = 0;
	if (!endoff) {
		endoff = ppc->pool->set_file->size;
		eof = true;
	}

	CHECK_INFO(ppc, "generating BTT Info headers at 0x%lx-0x%lx", startoff,
		endoff);
	uint64_t rawsize = endoff - startoff;

	/*
	 * Map the whole requested area in private mode as we want to write
	 * only BTT Info headers to file.
	 */
	void *addr = pool_set_file_map(ppc->pool->set_file, startoff);
	if (addr == MAP_FAILED) {
		status = CHECK_ERR(ppc, "Can not map file: %s",
			strerror(errno));
		goto error;
	}

	/* setup btt context */
	struct btt_context btt_context = {
		.ppc = ppc,
		.base_off = startoff,
		.len = rawsize
	};

	uint32_t lbasize = ppc->pool->hdr.blk.bsize;

	/* init btt in requested area */
	struct btt *bttp = btt_init(rawsize, lbasize,
		ppc->pool->hdr.pool.poolset_uuid, BTT_DEFAULT_NFREE,
		(void *)&btt_context, &ns_callbacks);

	if (!bttp) {
		status = CHECK_ERR(ppc, "cannot initialize BTT layer");
		goto error_unmap;
	}

	/* lazy layout writing */
	if (btt_write(bttp, 0, 0, addr)) {
		status = CHECK_ERR(ppc, "writing layout failed");
		goto error_btt;
	}

	/* add all arenas to list */
	struct arena *arenap = NULL;
	uint64_t offset = 0;
	uint64_t nextoff = 0;
	do {
		offset += nextoff;
		struct btt_info info;
		pool_read(ppc->pool, &info, sizeof(info), startoff + offset);

		if (pool_btt_info_valid(&info) != 1) {
			status = CHECK_ERR(ppc, "writing layout failed");
			goto error_btt;
		}
		arenap = malloc(sizeof (struct arena));
		if (!arenap) {
			status = CHECK_ERR(ppc,
				"cannot allocate memory for arena");
			goto error_btt;
		}
		memset(arenap, 0, sizeof (*arenap));
		arenap->offset = offset + startoff;
		arenap->valid = true;
		arenap->id = ppc->pool->narenas;
		memcpy(&arenap->btt_info, &info, sizeof (arenap->btt_info));

		check_insert_arena(ppc, arenap);

		nextoff = le64toh(info.nextoff);
	} while (nextoff > 0);

	if (!eof) {
		/*
		 * It means that requested area is between two valid arenas
		 * so make sure the offsets are correct.
		 */
		nextoff = endoff - (startoff + offset);
		if (nextoff != le64toh(arenap->btt_info.infooff) +
				sizeof (arenap->btt_info)) {
			goto error_btt;
		} else {
			arenap->btt_info.nextoff = htole64(nextoff);
			util_checksum(&arenap->btt_info,
				sizeof (arenap->btt_info),
				&arenap->btt_info.checksum, 1);
		}

	}

	btt_fini(bttp);
	return status;

error_btt:
	btt_fini(bttp);
error_unmap:
	if (!ppc->pool->params.is_btt_dev)
		munmap(addr, rawsize);
error:
	ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
	return status;
}

/*
 * btt_info_gen_fix -- (internal) fix by regenerating BTT Info
 */
static int
btt_info_gen_fix(PMEMpoolcheck *ppc, struct check_instep *location,
	uint32_t question, void *ctx)
{
	ASSERTeq(ctx, NULL);
	ASSERTne(location, NULL);
	union location *loc = (union location *)location;
	int result = 0;

	switch (question) {
	case Q_REGENERATE:
		/*
		 * If recovering by BTT Info backup failed try to regenerate
		 * btt layout.
		 */
		result = btt_info_gen_fix_exe(ppc, loc);
		break;
	default:
		ERR("not implemented question id: %u", question);
	}

	return result;
}

/*
 * btt_info_final -- (internal) finalize arena check and jump to next one
 */
static void
btt_info_final(PMEMpoolcheck *ppc, union location *loc)
{
	if (ppc->args.repair && loc->advanced_repair) {
		if (loc->offset2)
			loc->nextoff = loc->offset2 - loc->offset;
		else
			loc->nextoff = 0;
		location_release(loc);
	} else {
		/* save offset and insert BTT to cache for next steps */
		loc->arena->offset = loc->offset;
		loc->arena->valid = true;
		check_insert_arena(ppc, loc->arena);
		loc->nextoff = le64toh(loc->arena->btt_info.nextoff);
	}
}

struct step {
	int (*check)(PMEMpoolcheck *, union location *loc);
	int (*fix)(PMEMpoolcheck *ppc, struct check_instep *location,
		uint32_t question, void *ctx);
	bool advanced;
};

static const struct step steps[] = {
	{
		.check		= btt_info_checksum,

	},
	{
		.check		= btt_info_backup,
	},
	{
		.fix		= btt_info_backup_fix,
	},
	{
		.check		= btt_info_gen,
		.advanced	= true,
	},
	{
		.fix		= btt_info_gen_fix,
		.advanced	= true
	},
	{
		.check		= NULL,
	},
};

/*
 * step -- (internal) perform single step according to its parameters
 */
static inline int
step(PMEMpoolcheck *ppc, union location *loc)
{
	const struct step *step = &steps[loc->step++];

	if (step->advanced && !loc->advanced_repair)
		return 0;

	int status = 0;
	if (step->fix != NULL) {
		if (!check_has_answer(ppc->data))
			return 0;

		status = check_answer_loop(ppc, (struct check_instep *)loc,
			NULL, step->fix);

		if (check_has_error(ppc->data)) {
			struct check_status *err = check_pop_error(ppc->data);
			check_status_release(ppc, err);
			status = CHECK_ERR(ppc,
				"arena %u: cannot repair BTT Info header",
				loc->arena->id);
			location_release(loc);
		}
	} else
		status = step->check(ppc, loc);

	return status;
}

/*
 * check_btt_info -- entry point for btt info check
 */
void
check_btt_info(PMEMpoolcheck *ppc)
{
	COMPILE_ERROR_ON(sizeof (union location) !=
		sizeof (struct check_instep));

	union location *loc =
		(union location *)check_step_location_get(ppc->data);

	if (!loc->offset) {
		CHECK_INFO(ppc, "checking BTT Info headers");
		loc->offset = BTT_ALIGNMENT;
		if (!ppc->pool->params.is_btt_dev)
			loc->offset += BTT_ALIGNMENT;
		loc->nextoff = 0;
	}

	do {
		if (ppc->result != PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS) {
			loc->offset += loc->nextoff;
			loc->offset2 = 0;
			loc->nextoff = 0;
			loc->advanced_repair = false;
			loc->step = 0;
		}

		while (loc->step != CHECK_STEP_COMPLETE &&
			(steps[loc->step].check != NULL ||
			steps[loc->step].fix != NULL)) {

			if (step(ppc, loc) || ppc->pool->blk_no_layout == 1)
				return;
		}

		btt_info_final(ppc, loc);
	} while (loc->nextoff > 0);
}
