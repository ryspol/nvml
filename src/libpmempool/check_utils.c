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
 * check_utils.c -- check utility functions
 */

#include <stdbool.h>
#include <stdint.h>
#include <stddef.h>
#include <string.h>
#include <sys/types.h>

#include "out.h"
#include "util.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check.h"
#include "check_utils.h"

#define	BTT_INFO_SIG	"BTT_ARENA_INFO\0"

/*
 * check_utils_pop_answer -- pop single answer from answers queue
 */
static struct check_status *
check_utils_pop_answer(struct check_data *data)
{
	struct check_status *ret = NULL;
	if (!TAILQ_EMPTY(&data->answers)) {
		ret = TAILQ_FIRST(&data->answers);
		TAILQ_REMOVE(&data->answers, ret, next);
	}
	return ret;
}

/*
 * check_utils_answer_loop -- loop through all available answers
 */
struct check_status *
check_utils_answer_loop(PMEMpoolcheck *ppc, struct check_instep_location *loc,
	void *ctx, struct check_status *(*callback)(PMEMpoolcheck *,
	struct check_instep_location *loc, uint32_t question, void *ctx))
{
	struct check_status *answer;
	struct check_status *result = NULL;

	while ((answer = check_utils_pop_answer(ppc->data)) != NULL) {
		if (answer->status.answer != PMEMPOOL_CHECK_ANSWER_YES) {
			result = CHECK_STATUS_ERR(ppc, "");
			goto error;
		}

		result = callback(ppc, loc, answer->status.question, ctx);
		if (result != NULL)
			goto error;

		ppc->result = PMEMPOOL_CHECK_RESULT_REPAIRED;
		check_status_release(ppc, answer);
	}

	return result;

error:
	check_status_release(ppc, answer);
	ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
	return result;
}

/*
 * check_utils_get_uuid_str -- returns uuid in human readable format
 */
const char *
check_utils_get_uuid_str(uuid_t uuid)
{
	static char uuid_str[UUID_STR_MAX] = {0, };

	int ret = util_uuid_to_string(uuid, uuid_str);
	if (ret != 0) {
		ERR("failed to covert uuid to string");
		return NULL;
	}
	return uuid_str;
}
/*
 * check_utils_btt_info_valid -- check consistency of BTT Info header
 */
int
check_utils_btt_info_valid(struct btt_info *infop)
{
	if (!memcmp(infop->sig, BTT_INFO_SIG, BTTINFO_SIG_LEN))
		return util_checksum(infop, sizeof (*infop), &infop->checksum,
			0);
	else
		return -1;
}

/*
 * check_utils_log_convert2h -- convert pmemlog structure to host byte order
 */
static void
check_utils_log_convert2h(struct pmemlog *plp)
{
	plp->start_offset = le64toh(plp->start_offset);
	plp->end_offset = le64toh(plp->end_offset);
	plp->write_offset = le64toh(plp->write_offset);
}

/*
 * check_utils_log_read -- read pmemlog header
 */
struct check_status *
check_utils_log_read(PMEMpoolcheck *ppc)
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
	check_utils_log_convert2h(&ppc->pool->hdr.log);

	return NULL;
}

/*
 * check_utils_blk_read -- read pmemblk header
 */
struct check_status *
check_utils_blk_read(PMEMpoolcheck *ppc)
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
