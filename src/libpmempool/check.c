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
 * check.c -- functions performing check
 */

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdbool.h>
#include <sys/stat.h>
#include <sys/queue.h>
#include <errno.h>

#include "out.h"
#include "util.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check.h"
#include "check_backup.h"
#include "check_pool_hdr.h"
#include "check_pmemx.h"
#include "check_btt_info.h"

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
		.type	= POOL_TYPE_BLK
				| POOL_TYPE_LOG
				| POOL_TYPE_UNKNOWN,
		.func	= check_pool_hdr,
		.part	= true,
	},
	{
		.type	= POOL_TYPE_BLK
				| POOL_TYPE_LOG,
		.func	= check_pmemx,
		.part	= false,
	},
	{
		.type	= POOL_TYPE_BLK,
		.func	= check_btt_info,
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
	ppc->msg = malloc(sizeof (char) * CHECK_MAX_MSG_STR_SIZE);
	if (ppc->msg == NULL) {
		ERR("!malloc");
		goto error_msg_malloc;
	}
	ppc->data = malloc(sizeof (*ppc->data));
	if (ppc->data == NULL) {
		ERR("!malloc");
		goto error_data_malloc;
	}
	ppc->pool = malloc(sizeof (*ppc->pool));
	if (ppc->pool == NULL) {
		ERR("!malloc");
		goto error_pool_malloc;
	}
	ppc->data->check_status_cache = NULL;
	ppc->data->error = NULL;
	ppc->data->step = 0;
	ppc->pool->narenas = 0;

	if (pool_parse_params(ppc, &ppc->pool->params, 0)) {
		if (errno)
			perror(ppc->path);
		else
			ERR("%s: cannot determine type of pool\n",
				ppc->path);
		goto error;
	}

	int rdonly = !ppc->repair || ppc->dry_run;
	ppc->pool->set_file = pool_set_file_open(ppc->path, rdonly, 0);
	if (ppc->pool->set_file == NULL) {
		perror(ppc->path);
		goto error;
	}

	TAILQ_INIT(&ppc->pool->arenas);
	TAILQ_INIT(&ppc->data->infos);
	TAILQ_INIT(&ppc->data->questions);
	TAILQ_INIT(&ppc->data->answers);
	return 0;

error:
	free(ppc->pool);
error_pool_malloc:
	free(ppc->data);
error_data_malloc:
	free(ppc->msg);
error_msg_malloc:
	return -1;
}

/*
 * check_pop_question -- pop single question from questions queue
 */
static struct check_status *
check_pop_question(struct check_data *data)
{
	struct check_status *ret = NULL;
	if (!TAILQ_EMPTY(&data->questions)) {
		ret = TAILQ_FIRST(&data->questions);
		TAILQ_REMOVE(&data->questions, ret, next);
	}
	return ret;
}

#define	CHECK_ANSWER_YES	"yes"
#define	CHECK_ANSWER_NO		"no"

/*
 * check_push_answer -- process answer and push it to answers queue
 */
static struct check_status *
check_push_answer(PMEMpoolcheck *ppc)
{
	if (ppc->data->check_status_cache == NULL)
		return NULL;

	struct pmempool_check_status *status =
		&ppc->data->check_status_cache->status;
	if (status->str.answer != NULL) {
		if (strcmp(status->str.answer, CHECK_ANSWER_YES) == 0) {
			status->answer = PMEMPOOL_CHECK_ANSWER_YES;
		} else if (strcmp(status->str.answer, CHECK_ANSWER_NO) == 0) {
			status->answer = PMEMPOOL_CHECK_ANSWER_NO;
		}
	}

	if (status->answer == PMEMPOOL_CHECK_ANSWER_EMPTY) {
		check_status_push(ppc, ppc->data->check_status_cache);
		ppc->data->check_status_cache = NULL;
		return CHECK_STATUS_ERR(ppc, "Answer must be either %s or %s",
			CHECK_ANSWER_YES, CHECK_ANSWER_NO);
	} else {
		TAILQ_INSERT_TAIL(&ppc->data->answers,
			ppc->data->check_status_cache, next);
		ppc->data->check_status_cache = NULL;
	}

	return NULL;
}

/*
 * check_has_answer -- check if any answer exists
 */
bool
check_has_answer(struct check_data *data)
{
	return !TAILQ_EMPTY(&data->answers);
}

/*
 * check_step -- perform single check step
 */
struct check_status *
check_step(PMEMpoolcheck *ppc)
{
	struct check_status *stat = NULL;

	if (ppc->result == PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS) {
		stat = check_push_answer(ppc);
		if (stat != NULL)
			return stat;

		ppc->data->check_status_cache = check_pop_question(ppc->data);
		if (ppc->data->check_status_cache != NULL)
			return ppc->data->check_status_cache;
		else
			ppc->result = PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS;
	}

	if (ppc->data->step == PMEMPOOL_CHECK_END)
		return NULL;

	const struct check_step *step = &check_steps[ppc->data->step];

	if (step->func == NULL)
		goto check_end;

	if (!(step->type & ppc->pool->params.type))
		goto check_skip;

	if (ppc->pool->params.is_part && !step->part)
		goto check_skip;

	stat = step->func(ppc);

	if (ppc->result != PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS) {
		++ppc->data->step;
		memset(&ppc->data->instep_location, 0,
			sizeof (struct check_instep_location));
	}

	switch (ppc->result) {
	case PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS:
		ppc->data->check_status_cache = check_pop_question(ppc->data);
		return ppc->data->check_status_cache;
	case PMEMPOOL_CHECK_RESULT_CONSISTENT:
	case PMEMPOOL_CHECK_RESULT_REPAIRED:
		return stat;
	case PMEMPOOL_CHECK_RESULT_NOT_CONSISTENT:
		/*
		 * don't continue if pool is not consistent
		 * and we don't want to repair
		 */
		if (!ppc->repair)
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
	ppc->data->step = PMEMPOOL_CHECK_END;
	return stat;

check_skip:
	++ppc->data->step;
	return NULL;
}

/*
 * check_clear_arenas -- clear list of arenas
 */
static void
check_clear_arenas(struct pool_data *pool)
{
	while (!TAILQ_EMPTY(&pool->arenas)) {
		struct arena *arenap = TAILQ_FIRST(&pool->arenas);
		if (arenap->map)
			free(arenap->map);
		if (arenap->flog)
			free(arenap->flog);
		TAILQ_REMOVE(&pool->arenas, arenap, next);
		free(arenap);
	}
}

/*
 * check_clear_statuses -- clear all statuses
 */
static void
check_clear_statuses(struct check_data *data)
{
	if (data->error != NULL) {
		free(data->error);
		data->error = NULL;
	}

	if (data->check_status_cache != NULL) {
		free(data->check_status_cache);
		data->check_status_cache = NULL;
	}

	while (!TAILQ_EMPTY(&data->infos)) {
		struct check_status *statp = TAILQ_FIRST(&data->infos);
		TAILQ_REMOVE(&data->infos, statp, next);
		free(statp);
	}

	while (!TAILQ_EMPTY(&data->questions)) {
		struct check_status *statp = TAILQ_FIRST(&data->questions);
		TAILQ_REMOVE(&data->questions, statp, next);
		free(statp);
	}

	while (!TAILQ_EMPTY(&data->answers)) {
		struct check_status *statp = TAILQ_FIRST(&data->answers);
		TAILQ_REMOVE(&data->answers, statp, next);
		free(statp);
	}
}

/*
 * check_fini -- stop check process
 */
void
check_fini(PMEMpoolcheck *ppc)
{
	pool_set_file_close(ppc->pool->set_file);
	check_clear_arenas(ppc->pool);
	check_clear_statuses(ppc->data);
	free(ppc->pool);
	free(ppc->data);
	free(ppc->msg);
}

/*
 * check_status_create -- create single status, push it to proper queue
 */
struct check_status *
check_status_create(PMEMpoolcheck *ppc, enum pmempool_check_msg_type type,
	uint32_t question, const char *fmt, ...)
{
	struct check_status *st = malloc(sizeof (*st));

	if (ppc->flags & PMEMPOOL_CHECK_FORMAT_STR) {
		va_list ap;
		va_start(ap, fmt);
		vsnprintf(ppc->msg, CHECK_MAX_MSG_STR_SIZE, fmt, ap);
		va_end(ap);

		st->status.str.msg = ppc->msg;
		st->status.type = type;
	}

	switch (type) {
	case PMEMPOOL_CHECK_MSG_TYPE_ERROR:
		ASSERTeq(ppc->data->error, NULL);
		ppc->data->error = st;
		break;
	case PMEMPOOL_CHECK_MSG_TYPE_INFO:
		TAILQ_INSERT_TAIL(&ppc->data->infos, st, next);
		break;
	case PMEMPOOL_CHECK_MSG_TYPE_QUESTION:
		st->status.question = question;
		if (ppc->always_yes) {
			ppc->result = PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS;
			st->status.answer = PMEMPOOL_CHECK_ANSWER_YES;
			TAILQ_INSERT_TAIL(&ppc->data->answers, st, next);
		} else {
			ppc->result = PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS;
			st->status.answer = PMEMPOOL_CHECK_ANSWER_EMPTY;
			TAILQ_INSERT_TAIL(&ppc->data->questions, st, next);
		}
		break;
	}

	return st;
}

/*
 * check_status_release -- release single status object
 */
void
check_status_release(PMEMpoolcheck *ppc, struct check_status *status)
{
	if (status->status.type == PMEMPOOL_CHECK_MSG_TYPE_ERROR)
		ppc->data->error = NULL;
	free(status);
}

/*
 * check_status_push -- push single answer to answers queue
 */
void
check_status_push(PMEMpoolcheck *ppc, struct check_status *st)
{
	ASSERTeq(st->status.type, PMEMPOOL_CHECK_MSG_TYPE_QUESTION);
	TAILQ_INSERT_TAIL(&ppc->data->answers, st, next);
}

/*
 * check_memory -- check if memory contains single value
 */
int
check_memory(const uint8_t *buff, size_t len, uint8_t val)
{
	size_t i;
	for (i = 0; i < len; i++) {
		if (buff[i] != val)
			return -1;
	}

	return 0;
}

#define	STR_MAX 256
#define	TIME_STR_FMT "%a %b %d %Y %H:%M:%S"

/*
 * check_get_time_str -- returns time in human-readable format
 */
const char *
check_get_time_str(time_t time)
{
	static char str_buff[STR_MAX] = {0, };
	struct tm *tm = localtime(&time);

	if (tm)
		strftime(str_buff, STR_MAX, TIME_STR_FMT, tm);
	else
		snprintf(str_buff, STR_MAX, "unknown");

	return str_buff;
}

struct check_status *
check_questions_sequence_validate(PMEMpoolcheck *ppc)
{
	ASSERT(ppc->result == PMEMPOOL_CHECK_RESULT_CONSISTENT ||
		ppc->result == PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS ||
		ppc->result == PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS);
	if (ppc->result == PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS)
		return ppc->data->questions.tqh_first;
	else
		return NULL;
}

/*
 * pmempool_check_insert_arena -- insert arena to list
 */
void
check_insert_arena(PMEMpoolcheck *ppc, struct arena *arenap)
{
	TAILQ_INSERT_TAIL(&ppc->pool->arenas, arenap, next);
	ppc->pool->narenas++;
}
