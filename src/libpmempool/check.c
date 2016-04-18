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
 * check.c -- functions performing checks in proper order
 */

#include <stdio.h>
#include <stdint.h>

#include "out.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check.h"
#include "check_backup.h"
#include "check_pool_hdr.h"
#include "check_pmemx.h"
#include "check_btt_info.h"
#include "check_btt_map_flog.h"
#include "check_write.h"

struct check_step {
	struct check_status *(*func)(PMEMpoolcheck *);
	enum pool_type type;
	bool part;
};

static const struct check_step check_steps[] = {
	{
		.type	= POOL_TYPE_ALL,
		.func	= check_backup,
		.part	= true,
	},
	{
		.type	= POOL_TYPE_BLK | POOL_TYPE_LOG | POOL_TYPE_UNKNOWN,
		.func	= check_pool_hdr,
		.part	= true,
	},
	{
		.type	= POOL_TYPE_BLK | POOL_TYPE_LOG,
		.func	= check_pmemx,
		.part	= false,
	},
	{
		.type	= POOL_TYPE_BLK,
		.func	= check_btt_info,
		.part	= false,
	},
	{
		.type	= POOL_TYPE_BLK,
		.func	= check_btt_map_flog,
		.part	= false,
	},
	{
		.type	= POOL_TYPE_BLK | POOL_TYPE_LOG,
		.func	= check_write,
		.part	= false,
	},
	{
		.func	= NULL,
	},
};

/*
 * check_init -- initialize check process
 */
int
check_init(PMEMpoolcheck *ppc)
{
	if (!(ppc->msg = check_msg_alloc()))
		goto error_msg_malloc;
	if (!(ppc->data = check_data_alloc()))
		goto error_data_malloc;
	if (!(ppc->pool = pool_data_alloc(ppc)))
		goto error_pool_malloc;

	return 0;

error_pool_malloc:
	check_data_free(ppc->data);
error_data_malloc:
	free(ppc->msg);
error_msg_malloc:
	return -1;
}

/*
 * check_step -- perform single check step
 */
struct check_status *
check_step(PMEMpoolcheck *ppc)
{
	struct check_status *stat = NULL;

	if (ppc->result == PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS) {
		if ((stat = check_push_answer(ppc)))
			return stat;

		if ((stat = check_pop_question(ppc->data)))
			return stat;
		else
			ppc->result = PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS;
	}

	if (check_ended(ppc->data))
		return NULL;

	const struct check_step *step = &check_steps[check_step_get(ppc->data)];

	if (step->func == NULL)
		goto check_end;

	if (!(step->type & ppc->pool->params.type))
		goto check_skip;

	if (ppc->pool->params.is_part && !step->part)
		goto check_skip;

	stat = step->func(ppc);

	if (ppc->result != PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS) {
		check_step_inc(ppc->data);
	}

	switch (ppc->result) {
	case PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS:
		return check_pop_question(ppc->data);
	case PMEMPOOL_CHECK_RESULT_CONSISTENT:
	case PMEMPOOL_CHECK_RESULT_REPAIRED:
		return stat;
	case PMEMPOOL_CHECK_RESULT_NOT_CONSISTENT:
		/*
		 * don't continue if pool is not consistent
		 * and we don't want to repair
		 */
		if (!ppc->args.repair)
			goto check_end;
		else
			return stat;
	case PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR:
	case PMEMPOOL_CHECK_RESULT_ERROR:
		goto check_end;
	default:
		goto check_end;
	}

check_end:
	check_end(ppc->data);
	return stat;

check_skip:
	check_step_inc(ppc->data);
	return NULL;
}

/*
 * check_fini -- stop check process
 */
void
check_fini(PMEMpoolcheck *ppc)
{
	pool_data_free(ppc->pool);
	check_data_free(ppc->data);
	free(ppc->msg);
}
