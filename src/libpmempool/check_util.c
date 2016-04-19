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

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>

#include "out.h"
#include "libpmempool.h"
#include "pmempool.h"
#include "pool.h"
#include "check_util.h"

/* queue of check statuses */
struct check_status {
	TAILQ_ENTRY(check_status) next;
	struct pmempool_check_status status;
	char *msg;
};

TAILQ_HEAD(check_status_head, check_status);

/* check control context */
struct check_data {
	uint32_t step;
	struct check_instep_location instep_location;

	struct check_status *error;
	struct check_status_head infos;
	struct check_status_head questions;
	struct check_status_head answers;

	struct check_status *check_status_cache;
};

struct check_data *
check_data_alloc()
{
	struct check_data *data = (struct check_data *)malloc(sizeof (*data));
	if (data == NULL) {
		ERR("!malloc");
		return NULL;
	}

	data->check_status_cache = NULL;
	data->error = NULL;
	data->step = 0;

	TAILQ_INIT(&data->infos);
	TAILQ_INIT(&data->questions);
	TAILQ_INIT(&data->answers);

	return data;
}

void
check_data_free(struct check_data *data)
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

	free(data);
}

uint32_t
check_step_get(struct check_data *data)
{
	return data->step;
}

void
check_step_inc(struct check_data *data)
{
	++data->step;
	memset(&data->instep_location, 0,
		sizeof (struct check_instep_location));
}

struct check_instep_location *
check_step_location_get(struct check_data *data)
{
	return &data->instep_location;
}

#define	CHECK_END UINT32_MAX

void
check_end(struct check_data *data)
{
	// ASSERT(data->step != CHECK_END);
	data->step = CHECK_END;
}

int
check_ended(struct check_data *data)
{
	return data->step == CHECK_END;
}

#define	MSG_SEPARATOR	'|'
#define	MSG_PLACE_OF_SEPARATION	'.'
#define	MAX_MSG_STR_SIZE 8192

static inline struct check_status *
status_alloc()
{
	struct check_status *status = malloc(sizeof (*status));
	if (!status)
		FATAL("!malloc");
	status->msg = malloc(sizeof (char) * MAX_MSG_STR_SIZE);
	if (!status->msg) {
		free(status);
		FATAL("!malloc");
	}
	status->status.str.msg = status->msg;
	return status;
}

static void
status_release(struct check_status *status)
{
	free(status->msg);
	free(status);
}

static inline int
status_msg_trim(const char *msg)
{
	char *sep = strchr(msg, MSG_SEPARATOR);
	if (sep) {
		ASSERTne(sep, msg);
		--sep;
		ASSERTeq(*sep, MSG_PLACE_OF_SEPARATION);
		*sep = '\0';
		return 0;
	}
	return 1;
}

/*
 * check_status_create -- create single status, push it to proper queue
 *	MSG_SEPARATOR character in fmt is treated as message separator. If
 *	creating question but ppc arguments do not allow to make any changes
 *	(asking any question is pointless) it takes part of message before
 *	MSG_SEPARATOR character and use it to create error message. Character
 *	just before separator must be a MSG_PLACE_OF_SEPARATION character.
 */
struct check_status *
check_status_create(PMEMpoolcheck *ppc, enum pmempool_check_msg_type type,
	uint32_t question, const char *fmt, ...)
{
	struct check_status *st = status_alloc();
	struct check_status *info = NULL;

	if (ppc->args.flags & PMEMPOOL_CHECK_FORMAT_STR) {
		va_list ap;
		va_start(ap, fmt);
		int p = vsnprintf(st->msg, MAX_MSG_STR_SIZE, fmt, ap);
		va_end(ap);

		if (type != PMEMPOOL_CHECK_MSG_TYPE_QUESTION && errno &&
			p > 0) {
			snprintf(st->msg + p, MAX_MSG_STR_SIZE - (size_t)p,
				": %s", strerror(errno));
		}

		st->status.type = type;
	}

reprocess:
	switch (st->status.type) {
	case PMEMPOOL_CHECK_MSG_TYPE_ERROR:
		ASSERTeq(ppc->data->error, NULL);
		ppc->data->error = st;
		return st;
		break;
	case PMEMPOOL_CHECK_MSG_TYPE_INFO:
		TAILQ_INSERT_TAIL(&ppc->data->infos, st, next);
		break;
	case PMEMPOOL_CHECK_MSG_TYPE_QUESTION:

		if (!ppc->args.repair) {
			status_msg_trim(st->msg);
			st->status.type = PMEMPOOL_CHECK_MSG_TYPE_ERROR;
			goto reprocess;
		} else if (ppc->args.always_yes) {
			if (!status_msg_trim(st->msg)) {
				info = st;
				info->status.type =
					PMEMPOOL_CHECK_MSG_TYPE_INFO;
				st = status_alloc();
				st->status.question = question;
			}

			ppc->result = PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS;
			st->status.answer = PMEMPOOL_CHECK_ANSWER_YES;
			TAILQ_INSERT_TAIL(&ppc->data->answers, st, next);
		} else {
			st->status.question = question;
			ppc->result = PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS;
			st->status.answer = PMEMPOOL_CHECK_ANSWER_EMPTY;
			TAILQ_INSERT_TAIL(&ppc->data->questions, st, next);
		}
		if (ppc->args.always_yes) {
		} else {

		}
		break;
	}

	if (info) {
		st = info;
		info = NULL;
		goto reprocess;
	}

	return NULL;
}

/*
 * check_status_release -- release single status object
 */
void
check_status_release(PMEMpoolcheck *ppc, struct check_status *status)
{
	if (status->status.type == PMEMPOOL_CHECK_MSG_TYPE_ERROR)
		ppc->data->error = NULL;

	status_release(status);
}

static struct check_status *
check_pop_status(struct check_data *data, struct check_status_head *queue)
{
	ASSERTeq(data->check_status_cache, NULL);
	if (!TAILQ_EMPTY(queue)) {
		data->check_status_cache = TAILQ_FIRST(queue);
		TAILQ_REMOVE(queue, data->check_status_cache, next);
	}

	return data->check_status_cache;
}

/*
 * check_pop_question -- pop single question from questions queue
 */
struct check_status *
check_pop_question(struct check_data *data)
{
	return check_pop_status(data, &data->questions);
}

/*
 * check_pop_info -- pop single info from infos queue
 */
struct check_status *
check_pop_info(struct check_data *data)
{
	return check_pop_status(data, &data->infos);
}

/*
 * check_pop_error -- pop error from state
 */
struct check_status *
check_pop_error(struct check_data *data)
{
	data->check_status_cache = data->error;
	data->error = NULL;
	return data->check_status_cache;
}

void
check_clear_status_cache(struct check_data *data)
{
	if (data->check_status_cache) {
		enum pmempool_check_msg_type type =
			data->check_status_cache->status.type;
		if (type == PMEMPOOL_CHECK_MSG_TYPE_INFO ||
			type == PMEMPOOL_CHECK_MSG_TYPE_ERROR) {
			status_release(data->check_status_cache);
			data->check_status_cache = NULL;
		}
	}
}

/*
 * check_status_push -- push single answer to answers queue
 */
static void
status_push(struct check_data *data, struct check_status *st)
{
	ASSERTeq(st->status.type, PMEMPOOL_CHECK_MSG_TYPE_QUESTION);
	TAILQ_INSERT_TAIL(&data->answers, st, next);
}

#define	CHECK_ANSWER_YES	"yes"
#define	CHECK_ANSWER_NO		"no"

/*
 * check_push_answer -- process answer and push it to answers queue
 */
int
check_push_answer(PMEMpoolcheck *ppc)
{
	if (ppc->data->check_status_cache == NULL)
		return 0;

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
		status_push(ppc->data, ppc->data->check_status_cache);
		ppc->data->check_status_cache = NULL;
		CHECK_INFO(ppc, "Answer must be either %s or %s",
			CHECK_ANSWER_YES, CHECK_ANSWER_NO);
		return 1;
	} else {
		TAILQ_INSERT_TAIL(&ppc->data->answers,
			ppc->data->check_status_cache, next);
		ppc->data->check_status_cache = NULL;
	}

	return 0;
}

/*
 * check_has_info - check if any info exists
 */
bool
check_has_info(struct check_data *data)
{
	return !TAILQ_EMPTY(&data->infos);
}

/*
 * check_has_answer - check if any answer exists
 */
bool
check_has_answer(struct check_data *data)
{
	return !TAILQ_EMPTY(&data->answers);
}

/*
 * pop_answer -- pop single answer from answers queue
 */
static struct check_status *
pop_answer(struct check_data *data)
{
	struct check_status *ret = NULL;
	if (!TAILQ_EMPTY(&data->answers)) {
		ret = TAILQ_FIRST(&data->answers);
		TAILQ_REMOVE(&data->answers, ret, next);
	}
	return ret;
}

struct pmempool_check_status *
check_status_get(struct check_status *status)
{
	return &status->status;
}

int
check_status_is(struct check_status *status, enum pmempool_check_msg_type type)
{
	return status != NULL && status->status.type == type;
}

/*
 * check_answer_loop -- loop through all available answers
 */
struct check_status *
check_answer_loop(PMEMpoolcheck *ppc, struct check_instep_location *loc,
	void *ctx, struct check_status *(*callback)(PMEMpoolcheck *,
	struct check_instep_location *loc, uint32_t question, void *ctx))
{
	struct check_status *answer;
	struct check_status *result = NULL;

	while ((answer = pop_answer(ppc->data)) != NULL) {
		if (answer->status.answer != PMEMPOOL_CHECK_ANSWER_YES) {
			result = CHECK_ERR(ppc, "");
			goto cannot_repair;
		}

		result = callback(ppc, loc, answer->status.question, ctx);
		if (result != NULL)
			goto cannot_repair;
		if (ppc->result == PMEMPOOL_CHECK_RESULT_ERROR)
			goto error;

		ppc->result = PMEMPOOL_CHECK_RESULT_REPAIRED;
		check_status_release(ppc, answer);
	}

	return result;

error:
	check_status_release(ppc, answer);
	return result;

cannot_repair:
	check_status_release(ppc, answer);
	ppc->result = PMEMPOOL_CHECK_RESULT_CANNOT_REPAIR;
	return result;
}

struct check_status *
check_questions_sequence_validate(PMEMpoolcheck *ppc)
{
	ASSERT(ppc->result == PMEMPOOL_CHECK_RESULT_CONSISTENT ||
		ppc->result == PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS ||
		ppc->result == PMEMPOOL_CHECK_RESULT_PROCESS_ANSWERS ||
		ppc->result == PMEMPOOL_CHECK_RESULT_REPAIRED);
	if (ppc->result == PMEMPOOL_CHECK_RESULT_ASK_QUESTIONS)
		return ppc->data->questions.tqh_first;
	else
		return NULL;
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

#define	UUID_STR_MAX 37

/*
 * check_get_uuid_str -- returns uuid in human readable format
 */
const char *
check_get_uuid_str(uuid_t uuid)
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
 * pmempool_check_insert_arena -- insert arena to list
 */
void
check_insert_arena(PMEMpoolcheck *ppc, struct arena *arenap)
{
	TAILQ_INSERT_TAIL(&ppc->pool->arenas, arenap, next);
	ppc->pool->narenas++;
}
